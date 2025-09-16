import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import requests
from rmf_fleet_msgs.msg import FleetState, RobotState, Location
from geometry_msgs.msg import PoseStamped
import json
from rclpy.qos import QoSProfile
from rclpy.qos import QoSHistoryPolicy as History
from rclpy.qos import QoSDurabilityPolicy as Durability
from rclpy.qos import QoSReliabilityPolicy as Reliability
import numpy as np

class SimplePublisher(Node):
    def __init__(self):
        super().__init__('simple_publisher')
        self.timer = self.create_timer(1.0, self.timer_callback)
        self.robot_pose = {}
        self.robot_goals = []
        self.initialized = False
        self.robot_speed = 0.5  # m/s

        volatile_qos = QoSProfile(
            history=History.KEEP_LAST,
            depth=100,
            reliability=Reliability.RELIABLE,
            durability=Durability.VOLATILE)

        self.fleet_state_pub = self.create_publisher(
            FleetState,
            'fleet_states',
            qos_profile=volatile_qos)
        
        self.subscription = self.create_subscription(
            PoseStamped,
            'goal_pose',
            self.listener_callback,
            5)
        self.subscription  # prevent unused variable warning

    def listener_callback(self, msg):
        if not self.initialized:
            self.robot_pose['x'] = msg.pose.position.x
            self.robot_pose['y'] = msg.pose.position.y
            self.initialized = True
        else:
            self.robot_goals.append([msg.pose.position.x, msg.pose.position.y])

    def timer_callback(self):
        if not self.initialized:
            return

        if len(self.robot_goals) > 0:
            robot_goal = self.robot_goals[0]
            if self.robot_pose['x'] != robot_goal[0]:
                self.robot_pose['x'] += np.sign(robot_goal[0]-self.robot_pose['x']) * min(np.abs(robot_goal[0] - self.robot_pose['x']), self.robot_speed)

            if self.robot_pose['y'] != robot_goal[1]:
                self.robot_pose['y'] += np.sign(robot_goal[1]-self.robot_pose['y']) * min(np.abs(robot_goal[1] - self.robot_pose['y']), self.robot_speed)

        robot_state = RobotState()
        robot_state.name = 'omnibot_1'
        robot_state.model = 'omnibot_model'
        robot_state.task_id = ''
        robot_state.seq = 1
        robot_state.battery_percent = 100.0
        robot_state.location.x = self.robot_pose['x']
        robot_state.location.y = self.robot_pose['y']
        robot_state.location.yaw = 0.0
        robot_state.location.level_name = 'L1'

        for robot_goal in self.robot_goals:
            robot_location = Location()
            robot_location.x = robot_goal[0]
            robot_location.y = robot_goal[1]
            robot_location.level_name = 'L1'
            robot_state.path.append(robot_location)

        fleet_state = FleetState()
        fleet_state.name = 'omnibot_fleet'
        fleet_state.robots.append(robot_state)

        self.fleet_state_pub.publish(fleet_state)
        self.get_logger().info(f'Publishing: "{fleet_state.name}"')

def main(args=None):
    rclpy.init(args=args)
    node = SimplePublisher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
