import sys
import yaml
import datetime
import argparse
import bisect
import math
import os
import time
from functools import total_ordering
from zoneinfo import ZoneInfo

import rclpy
from rclpy.node import Node, Publisher
from rclpy.qos import (
    QoSProfile, QoSHistoryPolicy, QoSReliabilityPolicy, QoSDurabilityPolicy
)

from rmf_fleet_msgs.msg import (
    ChargingAssignment, ChargingAssignments, FleetState, RobotMode
)

@total_ordering
class ScheduleTimePoint:
    hour: int
    minute: int

    def __init__(self, hour: int, minute: int):
        self.hour = hour
        self.minute = minute

    def parse(text: str):
        segments = text.split(":")
        assert len(segments) == 2, (
            f'Time point text [{text}] does not have the correct HH:MM format'
        )
        hh = int(segments[0])
        mm = int(segments[1])
        assert 0 <= hh and hh < 24, (
            f'Time point text [{text}] has an hour value which is outside the '
            f'valid range of 0 -> 23.'
        )
        assert 0 <= mm and mm < 60, (
            f'Time point text [{text}] has a minute value which is outside the '
            f'valid range of 0 -> 59'
        )
        return ScheduleTimePoint(hh, mm)

    def __eq__(self, other):
        return self.hour == other.hour and self.minute == other.minute

    def __lt__(self, other):
        if self.hour < other.hour:
            return True
        elif self.hour > other.hour:
            return False
        return self.minute < other.minute

    def __hash__(self):
        return hash((self.hour, self.minute))


class Assignment:
    fleet: str
    robot: str
    charger: str

    def __init__(self, fleet, robot, charger):
        self.fleet = fleet
        self.robot = robot
        self.charger = charger


def publish_assignments(
    publisher: Publisher,
    assignments: dict[dict[str]],
    parking: list[str],
    overrides: dict | None = None,
):
    for fleet, robots in assignments.items():
        msg = ChargingAssignments()
        msg.fleet_name = fleet
        for robot, charger in robots.items():
            assignment = ChargingAssignment()
            assignment.robot_name = robot
            assignment.waypoint_name = charger

            override = (overrides or {}).get(fleet, {}).get(robot)
            if override is not None:
                # Standby robot that needs charging: back to its real charger
                assignment.waypoint_name = override
                assignment.mode = ChargingAssignment.MODE_CHARGE
            elif charger in parking:
                assignment.mode = ChargingAssignment.MODE_WAIT
            else:
                assignment.mode = ChargingAssignment.MODE_CHARGE
            msg.assignments.append(assignment)

        publisher.publish(msg)

def update_assignments(
    last_update_index: int | None,
    next_update_index: int,
    sorted_times: list,
    schedule: dict,
    assignments: dict,
    parking: list[str],
    publisher: Publisher,
    node: Node,
    chargers: dict | None = None,
    overrides: dict | None = None,
    log: bool = True,
):
    for key in sorted_times[last_update_index:next_update_index]:
        changes: list[Assignment] = schedule[key]
        for change in changes:
            assignments.setdefault(change.fleet, {})[change.robot] = change.charger
            if chargers is not None and change.charger not in parking:
                # fleet -> robot -> its last real charger in the schedule
                chargers.setdefault(change.fleet, {})[change.robot] = change.charger
            if log:
                node.get_logger().info(
                    f'Sending {change.fleet}/{change.robot} to {change.charger} at '
                    f'{key.hour:02d}:{key.minute:02d}'
                )
    if publisher is not None:
        publish_assignments(publisher, assignments, parking, overrides)


class StandbyBatteryGuard:
    """Standby points are published as parking spots (MODE_WAIT): RMF itself
    moves the robot there with its idle/park behaviour, so no task is created,
    and the robot shows as idle. But while parked, RMF treats the standby
    point as the robot's charger, so a charge RMF decides on would go there and
    never finish. This guard watches the robots' battery in fleet_states and,
    when a robot at a standby assignment needs charging, assigns its real
    charger back (MODE_CHARGE) so RMF charges it there. Once it is back at its
    charger with the fleet's recharge_soc, the standby point is assigned again.

    A robot needs charging when RMF has started charging it (mode CHARGING away
    from its real charger), or when it is idle below 1.2 x the fleet's
    recharge_threshold (the level at which RMF would retreat an idle robot to
    its charger). This node never sends tasks: all moves are RMF's own.
    """

    def __init__(self, node: Node, waypoints: dict, standby: list[str],
                 battery: dict, config_chargers: dict, tolerance: float,
                 stale_after: float):
        self.node = node
        self.waypoints = waypoints  # name -> (level, x, y)
        self.standby = set(standby)
        self.battery = battery  # fleet -> (low %, recharge_soc %)
        self.config_chargers = config_chargers  # fleet -> robot -> charger
        self.tolerance = tolerance
        self.stale_after = stale_after
        self.fleets = {}  # fleet -> (receive time, robot name -> RobotState)
        self.stale_warned = set()
        # fleet -> robot -> real charger assigned instead of its standby point
        self.overrides = {}
        node.create_subscription(
            FleetState, 'fleet_states', self._on_fleet_state,
            QoSProfile(
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
            ))

    def _on_fleet_state(self, msg: FleetState):
        self.fleets[msg.name] = (
            time.monotonic(), {r.name: r for r in msg.robots})

    def _fleet_robots(self, fleet: str):
        entry = self.fleets.get(fleet)
        if entry is None or time.monotonic() - entry[0] > self.stale_after:
            if fleet not in self.stale_warned:
                self.node.get_logger().warn(
                    f'No recent fleet_states for [{fleet}]; its standby '
                    f'battery checks are paused')
                self.stale_warned.add(fleet)
            return None
        if fleet in self.stale_warned:
            self.node.get_logger().info(f'fleet_states for [{fleet}] received again')
            self.stale_warned.discard(fleet)
        return entry[1]

    def _at(self, state, wp: str) -> bool:
        p = self.waypoints.get(wp)
        if p is None:
            return False
        level, x, y = p
        loc = state.location
        return (loc.level_name == level and
                math.hypot(loc.x - x, loc.y - y) <= self.tolerance)

    def check(self, assignments: dict, chargers: dict) -> bool:
        """Update the overrides; True if they changed."""
        changed = False
        for fleet, robots in assignments.items():
            live = self._fleet_robots(fleet)
            overrides = self.overrides.setdefault(fleet, {})
            for robot, wp in robots.items():
                if wp not in self.standby:
                    if overrides.pop(robot, None) is not None:
                        changed = True
                    continue
                if live is None or robot not in live:
                    continue  # keep the current assignment until we know more
                state = live[robot]
                charger = (chargers.get(fleet, {}).get(robot)
                           or self.config_chargers.get(fleet, {}).get(robot))
                if charger is None:
                    self.node.get_logger().error(
                        f'{fleet}/{robot} has no real charger (none in the '
                        f'schedule or the fleet config); it cannot be sent to '
                        f'charge from standby')
                    continue
                low, recharged = self.battery[fleet]
                battery = state.battery_percent
                if robot in overrides:
                    if self._at(state, charger) and battery >= recharged:
                        del overrides[robot]
                        changed = True
                        self.node.get_logger().info(
                            f'{fleet}/{robot}: charged to {battery:.0f}%, '
                            f'standby point {wp} assigned again')
                    continue
                rmf_charging = (state.mode.mode == RobotMode.MODE_CHARGING
                                and not self._at(state, charger))
                idle_low = not state.task_id and battery < low
                if rmf_charging or idle_low:
                    overrides[robot] = charger
                    changed = True
                    why = ('RMF wants to charge it' if rmf_charging else
                           f'battery {battery:.0f}% is below {low:.0f}%')
                    self.node.get_logger().info(
                        f'{fleet}/{robot}: {why}; assigning its real charger '
                        f'{charger} instead of standby point {wp}')
        return changed


def load_fleet_configs(paths: list[str]) -> dict:
    """fleet name -> {'threshold', 'soc', 'chargers': robot -> charger} from
    fleet adapter config files (rmf_fleet section)."""
    out = {}
    for path in paths:
        if not path or not os.path.isfile(path):
            continue
        with open(path, 'r') as f:
            rmf_fleet = (yaml.safe_load(f) or {}).get('rmf_fleet') or {}
        name = rmf_fleet.get('name')
        if not name:
            continue
        robots = rmf_fleet.get('robots') or {}
        out[name] = {
            'threshold': rmf_fleet.get('recharge_threshold'),
            'soc': rmf_fleet.get('recharge_soc'),
            'chargers': {
                r: c['charger'] for r, c in robots.items()
                if isinstance(c, dict) and c.get('charger')
            },
        }
    return out


def load_waypoints(paths: list[str]) -> dict:
    """waypoint name -> (level, x, y) from RMF nav graph yaml files."""
    out = {}
    for path in paths:
        if not path or not os.path.isfile(path):
            continue
        with open(path, 'r') as f:
            graph = yaml.safe_load(f)
        for level, data in (graph.get('levels') or {}).items():
            for v in data.get('vertices', []):
                name = (v[2] or {}).get('name') if len(v) > 2 else None
                if name:
                    out[name] = (level, float(v[0]), float(v[1]))
    return out


def simulation_time(node: Node) -> ScheduleTimePoint:
    seconds, _ = node.get_clock().now().seconds_nanoseconds()
    minutes: int = int(seconds/60)
    hour: int = int((minutes/60) % 24)
    minute = minutes % 60
    return ScheduleTimePoint(hour, minute)


def real_time(node: Node, timezone: ZoneInfo) -> ScheduleTimePoint:
    nanoseconds = float(node.get_clock().now().nanoseconds)
    seconds = nanoseconds / 1e9
    dt = datetime.datetime.fromtimestamp(seconds, timezone)
    return ScheduleTimePoint(dt.hour, dt.minute)


def main(argv=sys.argv):
    rclpy.init(args=argv)
    node = Node("rmf_charging_schedule")
    use_sim_time = node.get_parameter('use_sim_time').value

    args_without_ros = rclpy.utilities.remove_ros_args(argv)

    parser = argparse.ArgumentParser(
        prog='rmf_charging_schedule',
        description='Manage a fixed 24-hour charger schedule rotation',
    )
    parser.add_argument(
        'schedule', type=str,
        help=(
            'A .yaml file representing the schedule. See README for the '
            'expected format.'
        )
    )
    parser.add_argument(
        '-z', '--timezone', type=str, required=False,
        help=(
            'Timezone that the 24-hour rotation will be based on. If not '
            'provided, the system\'s local timezone will be used.'
        )
    )
    parser.add_argument(
        '-t', '--test-time', action='store_true',
        help=(
            'Use this option to test the real time calculation by printing the '
            'current HH:MM based on your settings. This may be used in '
            'conjunction with the --timezone option and sim time. The node '
            'will immediately quit after printing the time, so this will not '
            'publish any assignment messages.'
        )
    )
    parser.add_argument(
        '-c', '--fleet-config', action='append', default=[],
        help=(
            'Fleet adapter config .yaml, for the standby battery levels '
            '(recharge_threshold, recharge_soc) and the robots\' real chargers. '
            'May be given more than once. Defaults to the files named by '
            'environment variables ending in ADAPTER_CONFIG_FILE.'
        )
    )
    parser.add_argument(
        '-g', '--nav-graph', action='append', default=[],
        help=(
            'Nav graph .yaml used to look up charger positions. May be given '
            'more than once. Defaults to the files named by environment '
            'variables starting with RMF_NAV_GRAPH.'
        )
    )
    parser.add_argument(
        '--tolerance', type=float, default=1.0,
        help='Distance (m) at which a robot counts as being at its charger.'
    )
    parser.add_argument(
        '--stale-after', type=float, default=30.0,
        help=(
            'Seconds without fleet_states from a fleet after which its standby '
            'battery checks pause.'
        )
    )

    args = parser.parse_args(args_without_ros[1:])
    schedule_file = args.schedule

    if args.timezone is not None:
        timezone = ZoneInfo(args.timezone)
    else:
        timezone = None

    if use_sim_time:
        get_time = lambda: simulation_time(node)
    else:
        get_time = lambda: real_time(node, timezone)

    if args.test_time:
        t = get_time()
        print(f'{t.hour:02d}:{t.minute:02d}')
        return

    with open(schedule_file, 'r') as f:
        schedule_yaml = yaml.safe_load(f)

    unsorted_schedule = {}
    parking = []
    standby = []
    for fleet, change in schedule_yaml.items():
        if fleet == 'parking':
            # We treat the parking entry as a special case that simply lists
            # which waypoints are parking spots
            parking = change
            continue
        if fleet == 'standby':
            # Standby points are parking spots too, but a robot that needs
            # charging there is given its real charger (StandbyBatteryGuard)
            standby = change
            continue

        for time_text, assignments in change.items():
            time_point = ScheduleTimePoint.parse(time_text)
            entry: list[Assignment] = unsorted_schedule.get(time_point, list())
            for robot, charger in assignments.items():
                entry.append(Assignment(fleet, robot, charger))
            unsorted_schedule[time_point] = entry

    schedule = {}
    sorted_times = []
    for time_point in sorted(unsorted_schedule.keys()):
        sorted_times.append(time_point)
        schedule[time_point] = unsorted_schedule[time_point]

    parking = list(parking) + [w for w in standby if w not in parking]

    num_fleets = len(schedule_yaml.keys())
    transient_qos = QoSProfile(
        history=QoSHistoryPolicy.KEEP_LAST,
        depth=2*num_fleets,
        reliability=QoSReliabilityPolicy.RELIABLE,
        durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
    )
    publisher = node.create_publisher(
        ChargingAssignments, 'charging_assignments', transient_qos
    )

    # fleet -> robot -> charger
    assignments = {}
    # fleet -> robot -> last real (non parking) charger in the schedule
    chargers = {}
    guard = None
    last_update_index = bisect.bisect_right(sorted_times, get_time())
    # The rotation repeats every day: replay yesterday's entries first
    # (silently), so every robot starts with its current assignment and real
    # charger even when the node starts before the first entry of the day
    update_assignments(
        None, None, sorted_times, schedule, assignments, parking, None, node,
        chargers, log=False,
    )
    # With standby points, the first publish waits for the first battery check
    # (in update()); RMF keeps its last assignments meanwhile
    update_assignments(
        None, last_update_index,
        sorted_times, schedule, assignments, parking,
        None if standby else publisher, node, chargers,
    )
    first_publish = bool(standby)

    if standby:
        graphs = list(args.nav_graph) or [
            v for k, v in os.environ.items() if k.startswith('RMF_NAV_GRAPH')
        ]
        config_paths = list(args.fleet_config) or [
            v for k, v in os.environ.items() if k.endswith('ADAPTER_CONFIG_FILE')
        ]
        fleet_configs = load_fleet_configs(config_paths)
        battery = {}
        for fleet in assignments:
            cfg = fleet_configs.get(fleet, {})
            threshold, soc = cfg.get('threshold'), cfg.get('soc')
            if threshold is None or soc is None:
                node.get_logger().warn(
                    f'No recharge_threshold / recharge_soc for fleet [{fleet}] '
                    f'in {config_paths}; using 0.35 / 0.75. Pass -c <fleet '
                    f'adapter config> to use the fleet\'s values.'
                )
                threshold = 0.35 if threshold is None else threshold
                soc = 0.75 if soc is None else soc
            battery[fleet] = (120.0 * float(threshold), 100.0 * float(soc))
            node.get_logger().info(
                f'[{fleet}] standby points {standby}: parked there by RMF; '
                f'given the real charger back when RMF wants to charge or when '
                f'idle below {battery[fleet][0]:.0f}%, standby again at '
                f'{battery[fleet][1]:.0f}%'
            )
        waypoints = load_waypoints(graphs)
        needed = set(standby)
        for fleet_chargers in list(chargers.values()) + [
                c['chargers'] for c in fleet_configs.values()]:
            needed.update(fleet_chargers.values())
        missing = sorted(w for w in needed if w not in waypoints)
        if missing:
            node.get_logger().error(
                f'Waypoints {missing} are not in the nav graphs {graphs}: '
                f'robots cannot be located at them, so a robot sent to charge '
                f'from standby will not be sent back. Pass -g <nav graph>.'
            )
        else:
            node.get_logger().info(
                f'Standby points and chargers found in nav graphs {graphs}')
        guard = StandbyBatteryGuard(
            node, waypoints, standby, battery,
            {f: c['chargers'] for f, c in fleet_configs.items()},
            args.tolerance, args.stale_after,
        )

    def overrides():
        return guard.overrides if guard is not None else None

    def update():
        nonlocal last_update_index
        nonlocal sorted_times
        nonlocal schedule
        nonlocal assignments
        nonlocal publisher
        nonlocal parking
        nonlocal first_publish

        next_update_index = bisect.bisect_right(sorted_times, get_time())
        changed = first_publish
        first_publish = False
        if last_update_index < next_update_index:
            update_assignments(
                last_update_index, next_update_index,
                sorted_times, schedule, assignments, parking, None, node,
                chargers,
            )
            last_update_index = next_update_index
            changed = True

        elif next_update_index < last_update_index:
            # The cycle must have restarted, e.g. passing midnight
            update_assignments(
                None, next_update_index,
                sorted_times, schedule, assignments, parking, None, node,
                chargers,
            )
            last_update_index = next_update_index
            changed = True

        # Battery check before publishing, so a robot that needs charging is
        # never sent to a standby point in the first place
        if guard is not None and guard.check(assignments, chargers):
            changed = True
        if changed:
            publish_assignments(publisher, assignments, parking, overrides())

    node.create_timer(10.0, update)

    rclpy.spin(node)

if __name__ == '__main__':
    main(sys.argv)

