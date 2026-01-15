"""Read Only Mutex Supervisor Node."""

import numpy as np

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy as Durability
from rclpy.qos import QoSHistoryPolicy as History
from rclpy.qos import QoSProfile
from rclpy.qos import QoSReliabilityPolicy as Reliability

from rmf_building_map_msgs.msg import Graph

from rmf_fleet_msgs.msg import FleetState, MutexGroupRequest

import argparse
import sys


def get_distance(x1, y1, x2, y2):
    """Calculate the Euclidean distance between two points."""
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5


class ReadOnlyMutexSupervisor(Node):
    """
    Read Only Mutex Supervisor Node.

    A simple read-only mutex supervisor that manages mutexes based on its
    fleet state and navigation graphs.
    """

    def __init__(self, fleet_name, threshold):
        """Initialize the read-only mutex supervisor node."""
        super().__init__('read_only_mutex_supervisor')

        self.fleet_name = fleet_name

        self.threshold = threshold
        self.mutex_wp_lib = []
        self.mutex_edge_lib = []
        self.fleet_state_subscription = self.create_subscription(
            FleetState, 'fleet_states', self.fleet_state_callback, 10
        )

        transient_qos = QoSProfile(
            history=History.KEEP_LAST,
            depth=10,
            reliability=Reliability.RELIABLE,
            durability=Durability.TRANSIENT_LOCAL,
        )

        self.graph_subscription = self.create_subscription(
            Graph, 'nav_graphs', self.graph_callback, transient_qos
        )

        self.mutex_publisher = self.create_publisher(
            MutexGroupRequest, 'mutex_group_request', transient_qos
        )
        self.mutex_lock = {}

        self.timer = self.create_timer(1.0, self.publisher_callback)

    def request_mutex(self, mutex):
        """Reqest a mutex for the robot."""
        request = MutexGroupRequest()
        request.group = mutex
        request.claimant = 999
        request.claim_time.sec = 0
        request.claim_time.nanosec = 0
        request.mode = MutexGroupRequest.MODE_LOCK
        self.mutex_publisher.publish(request)
        print(f'Requested mutex {mutex}')

    def release_mutex(self, mutex):
        """Release a mutex for the robot."""
        request = MutexGroupRequest()
        request.group = mutex
        request.claimant = 999
        request.claim_time.sec = 0
        request.claim_time.nanosec = 0
        request.mode = MutexGroupRequest.MODE_RELEASE
        self.mutex_publisher.publish(request)
        print(f'Released mutex {mutex}')

    def publisher_callback(self):
        """Publish the mutex requests for all robots."""
        for mutex in self.mutex_lock:
            self.request_mutex(mutex)

    def graph_callback(self, msg):
        """Get all the mutexes from the navigation graph."""
        self.get_logger().info('I heard: "%s"' % msg.name)
        for node in msg.vertices:
            for param in node.params:
                if param.name == 'mutex':
                    self.mutex_wp_lib.append(
                        [node.x, node.y, param.value_string]
                    )
                    self.get_logger().info(
                        f'Graph {msg.name} has node mutex: {param.value_string}'
                    )

        for edge in msg.edges:
            for param in edge.params:
                if param.name == 'mutex':
                    self.mutex_edge_lib.append(
                        [
                            msg.vertices[edge.v1_idx],
                            msg.vertices[edge.v2_idx],
                            param.value_string,
                        ]
                    )
                    self.get_logger().info(
                        f'Graph {msg.name} has edge mutex: {param.value_string}'
                    )

    def fleet_state_callback(self, msg):
        """Handle the fleet state and manage mutexes for robots."""
        if msg.name != self.fleet_name:
            return

        for robot in msg.robots:
            robot_in_mutex = False
            x = robot.location.x
            y = robot.location.y

            for mutex in self.mutex_wp_lib:
                distance = get_distance(x, y, mutex[0], mutex[1])
                if distance < self.threshold:
                    self.mutex_lock[mutex[2]] = robot.name
                    robot_in_mutex = True
                    print(
                        f'Acquired wp mutex {mutex[2]} for robot {robot.name}'
                    )

            for mutex in self.mutex_edge_lib:
                edge_x = np.linspace(mutex[0].x, mutex[1].x)
                edge_y = np.linspace(mutex[0].y, mutex[1].y)

                distance = np.sqrt(
                    np.power(edge_x - x, 2) + np.power(edge_y - y, 2)
                )

                if (distance < self.threshold).any():
                    self.mutex_lock[mutex[2]] = robot.name
                    robot_in_mutex = True
                    print(
                        f'Acquired wp mutex {mutex[2]} for robot {robot.name}'
                    )

            if not robot_in_mutex:
                for mutex in self.mutex_lock.keys():
                    if robot.name == self.mutex_lock[mutex]:
                        self.release_mutex(mutex)
                        del self.mutex_lock[mutex]
                        break


def main(argv=sys.argv):
    """Run the read-only mutex supervisor node."""
    rclpy.init(args=argv)
    args_without_ros = rclpy.utilities.remove_ros_args(argv)

    parser = argparse.ArgumentParser()
    parser.add_argument('-F', '--fleet-name', help='Read-only fleet name')
    parser.add_argument(
        '-t',
        '--threshold',
        type=float,
        default=0.1,
        help='Distance threshold for mutex acquisition',
    )
    args = parser.parse_args(args_without_ros[1:])

    read_only_node = ReadOnlyMutexSupervisor(args.fleet_name, args.threshold)
    rclpy.spin(read_only_node)
    read_only_node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
