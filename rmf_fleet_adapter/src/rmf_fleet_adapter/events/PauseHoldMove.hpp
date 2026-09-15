/*
 * Copyright (C) 2021 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

#ifndef SRC__RMF_FLEET_ADAPTER__EVENTS__PAUSEHOLDMOVE_HPP
#define SRC__RMF_FLEET_ADAPTER__EVENTS__PAUSEHOLDMOVE_HPP

#include "../agv/RobotContext.hpp"
#include "../Negotiator.hpp"

#include "../services/FindPath.hpp"

#include "ExecutePlan.hpp"

#include <rmf_task_sequence/Event.hpp>
#include <rmf_task/events/SimpleEventState.hpp>

namespace rmf_fleet_adapter {
namespace events {

//==============================================================================
/// Drives a robot to a specific waypoint (e.g. its charger) using the real
/// RMF planner, while the robot's normal active task is only interrupted, not
/// cancelled. Unlike EmergencyPullover, the destination is fixed by the
/// caller instead of searched for, and arriving does NOT finish the event --
/// it holds there until explicitly cancel()'d or kill()'d.
class PauseHoldMove : public rmf_task_sequence::Event
{
public:

  static rmf_task::Task::ActivePtr start(
    const std::string& task_id,
    agv::RobotContextPtr& context,
    rmf_traffic::agv::Plan::Goal goal,
    std::function<void(rmf_task::Phase::ConstSnapshotPtr)> update,
    std::function<void()> finished);

  class Standby : public rmf_task_sequence::Event::Standby
  {
  public:

    static std::shared_ptr<Standby> make(
      const AssignIDPtr& id,
      const agv::RobotContextPtr& context,
      rmf_traffic::agv::Plan::Goal goal,
      std::function<void()> update);

    Standby(rmf_traffic::agv::Plan::Goal goal)
    : _goal(std::move(goal)) {}

    ConstStatePtr state() const;

    rmf_traffic::Duration duration_estimate() const final;

    ActivePtr begin(
      std::function<void()> checkpoint,
      std::function<void()> finished) final;

  private:

    AssignIDPtr _assign_id;
    agv::RobotContextPtr _context;
    rmf_traffic::agv::Plan::Goal _goal;
    std::function<void()> _update;
    rmf_task::events::SimpleEventStatePtr _state;
    ActivePtr _active = nullptr;

  };

  class Active
    : public rmf_task_sequence::Event::Active,
    public std::enable_shared_from_this<Active>
  {
  public:

    static std::shared_ptr<Active> make(
      const AssignIDPtr& id,
      agv::RobotContextPtr context,
      rmf_traffic::agv::Plan::Goal goal,
      rmf_task::events::SimpleEventStatePtr state,
      std::function<void()> update,
      std::function<void()> finished);

    Active(rmf_traffic::agv::Plan::Goal goal)
    : _goal(std::move(goal)) {}

    ConstStatePtr state() const final;

    rmf_traffic::Duration remaining_time_estimate() const final;

    Backup backup() const final;

    Resume interrupt(std::function<void()> task_is_interrupted) final;

    void cancel() final;

    void kill() final;

  private:

    void _schedule_retry();

    void _find_plan();

    void _execute_plan(
      rmf_traffic::PlanId plan_id,
      rmf_traffic::agv::Plan plan,
      rmf_traffic::schedule::Itinerary full_itinerary);

    void _on_arrived();

    void _stop_and_clear();

    Negotiator::NegotiatePtr _respond(
      const Negotiator::TableViewerPtr& table_view,
      const Negotiator::ResponderPtr& responder);

    AssignIDPtr _assign_id;
    agv::RobotContextPtr _context;
    rmf_traffic::agv::Plan::Goal _goal;
    std::function<void()> _update;
    std::function<void()> _finished;
    rmf_task::events::SimpleEventStatePtr _state;
    std::shared_ptr<Negotiator> _negotiator;
    std::optional<ExecutePlan> _execution;

    std::shared_ptr<services::FindPath> _find_path_service;
    rmf_rxcpp::subscription_guard _plan_subscription;
    rmf_rxcpp::subscription_guard _replan_request_subscription;
    rclcpp::TimerBase::SharedPtr _find_path_timeout;
    rclcpp::TimerBase::SharedPtr _retry_timer;

    bool _is_interrupted = false;
    bool _is_holding = false;
  };

private:
  static rmf_task::Activator _make_activator(
    std::function<rmf_traffic::Time()> clock);
};

} // namespace events
} // namespace rmf_fleet_adapter

#endif // SRC__RMF_FLEET_ADAPTER__EVENTS__PAUSEHOLDMOVE_HPP
