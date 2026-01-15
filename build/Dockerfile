ARG TAG="latest"
FROM rosdep:$TAG

ARG NETRC

SHELL ["bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp

RUN apt update
RUN mkdir -p /opt/rmf/src
WORKDIR /opt/rmf
RUN echo ${NETRC} > /root/.netrc
RUN apt-get install nlohmann-json3-dev
# copy rmf source repos
COPY /build/rmf-src src

RUN rosdep update --rosdistro $ROS_DISTRO
RUN rosdep install --from-paths src --ignore-src --rosdistro $ROS_DISTRO \
    --skip-keys roscpp  \
    --skip-keys actionlib \
    --skip-keys rviz \
    --skip-keys catkin \
    --skip-keys move_base \
    --skip-keys amcl \
    --skip-keys turtlebot3_navigation \
    --skip-keys turtlebot3_bringup \
    --skip-keys move_base_msgs \
    --skip-keys dwa_local_planner \
    --skip-keys map_server \
    --skip-keys ignition \
    --skip-keys rmf_traffic_ros2 \
    --skip-keys rmf_reservation_msgs \
    --skip-keys rmf_fleet_adapter \
    --skip-keys rmf_reservation_node \
    -y

RUN . /opt/ros/$ROS_DISTRO/setup.sh \
  && colcon build --cmake-args -DCMAKE_BUILD_TYPE=Release -DNO_DOWNLOAD_MODELS=True

RUN sed -i '$isource "/opt/rmf/install/setup.bash"' /ros_entrypoint.sh

ENTRYPOINT ["/ros_entrypoint.sh"]
CMD ["bash"]
