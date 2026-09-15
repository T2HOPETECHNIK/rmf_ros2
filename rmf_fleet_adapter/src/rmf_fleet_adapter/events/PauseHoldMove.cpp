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

#include "PauseHoldMove.hpp"

#include <rmf_traffic/schedule/StubbornNegotiator.hpp>

#include <rmf_task_sequence/Task.hpp>
#include <rmf_task_sequence/phases/SimplePhase.hpp>
#include <rmf_task_sequence/events/Placeholder.hpp>
#include "../project_itinerary.hpp"

#include "internal_utilities.hpp"

namespace rmf_fleet_adapter {
namespace events {

//==============================================================================
// just holds the charger goal so it can be passed around.
class PauseHoldMoveDescription
  : public rmf_task_sequence::events::Placeholder::Description
{
public:
  PauseHoldMoveDescription(rmf_traffic::agv::Plan::Goal goal)
  : rmf_task_sequence::events::Placeholder::Description(
      "Pause Hold Move", ""),
    _goal(std::move(goal))
  {
    // Do nothing
  }

  const rmf_traffic::agv::Plan::Goal& goal() const
  {
    return _goal;
  }

private:
  rmf_traffic::agv::Plan::Goal _goal;
};

//==============================================================================
// builds one task with this one event as its only phase, hands it to the task activator.
rmf_task::Task::ActivePtr PauseHoldMove::start(
  const std::string& task_id,
  agv::RobotContextPtr& context,
  rmf_traffic::agv::Plan::Goal goal,
  std::function<void(rmf_task::Phase::ConstSnapshotPtr)> update,
  std::function<void()> finished)
{
  static auto activator = _make_activator(context->clock());
  rmf_task_sequence::Task::Builder builder;
  builder.add_phase(
    rmf_task_sequence::phases::SimplePhase::Description::make(
      std::make_shared<PauseHoldMoveDescription>(std::move(goal))), {});

  const auto desc = builder.build("Pause Hold Move", "");

  const auto time_now = context->now();
  rmf_task::Task::ConstBookingPtr booking =
    std::make_shared<const rmf_task::Task::Booking>(
    task_id,
    time_now,
    nullptr,
    context->requester_id(),
    time_now,
    true);
  const rmf_task::Request request(std::move(booking), desc);

  return activator.activate(
    context->make_get_state(),
    context->task_parameters(),
    request,
    std::move(update),
    [](const auto&) {},
    [](const auto&) {},
    std::move(finished));
}

//==============================================================================
//creates the waiting object, marks status Standby.
auto PauseHoldMove::Standby::make(
  const AssignIDPtr& id,
  const agv::RobotContextPtr& context,
  rmf_traffic::agv::Plan::Goal goal,
  std::function<void()> update) -> std::shared_ptr<Standby>
{
  auto standby = std::make_shared<Standby>(std::move(goal));
  standby->_assign_id = id;
  standby->_context = context;
  standby->_update = std::move(update);
  standby->_state = rmf_task::events::SimpleEventState::make(
    id->assign(),
    "Pause hold move",
    "",
    rmf_task::Event::Status::Standby,
    {},
    context->clock());

  return standby;
}

//==============================================================================
auto PauseHoldMove::Standby::state() const -> ConstStatePtr
{
  return _state;
}

//==============================================================================
//reports 0, since this holds forever until stopped.
rmf_traffic::Duration PauseHoldMove::Standby::duration_estimate() const
{
  // A pause hold move will last indefinitely until it gets cancelled, which
  // may happen at any time (resume, or an auto-cancel timeout).
  return rmf_traffic::Duration(0);
}

//==============================================================================
//switches from Standby to Active.
auto PauseHoldMove::Standby::begin(
  std::function<void()>,
  std::function<void()> finished) -> ActivePtr
{
  if (!_active)
  {
    _active = Active::make(
      _assign_id,
      _context,
      _goal,
      _state,
      _update,
      std::move(finished));
  }

  return _active;
}

//==============================================================================
// creates the running object, sets up traffic negotiation, listens for fleet-wide replan events, calls _find_plan() right away.
auto PauseHoldMove::Active::make(
  const AssignIDPtr& id,
  agv::RobotContextPtr context,
  rmf_traffic::agv::Plan::Goal goal,
  rmf_task::events::SimpleEventStatePtr state,
  std::function<void()> update,
  std::function<void()> finished) -> std::shared_ptr<Active>
{
  auto active = std::make_shared<Active>(std::move(goal));
  active->_assign_id = id;
  active->_context = std::move(context);
  active->_update = std::move(update);
  active->_finished = std::move(finished);
  active->_state = std::move(state);
  active->_negotiator =
    Negotiator::make(
    active->_context,
    [w = active->weak_from_this()](
      const auto& t, const auto& r) -> Negotiator::NegotiatePtr
    {
      if (const auto self = w.lock())
        return self->_respond(t, r);

      r->forfeit({});
      return nullptr;
    });

  active->_replan_request_subscription =
    active->_context->observe_replan_request()
    .observe_on(rxcpp::identity_same_worker(active->_context->worker()))
    .subscribe(
    [w = active->weak_from_this()](const auto&)
    {
      const auto self = w.lock();
      if (self && !self->_is_holding && !self->_find_path_service)
      {
        RCLCPP_INFO(
          self->_context->node()->get_logger(),
          "Replanning requested for [%s] during pause hold move",
          self->_context->requester_id().c_str());

        if (const auto c = self->_context->command())
          c->stop();

        self->_find_plan();
      }
    });

  active->_find_plan();

  return active;
}

//==============================================================================
auto PauseHoldMove::Active::state() const -> ConstStatePtr
{
  return _state;
}

//==============================================================================
rmf_traffic::Duration PauseHoldMove::Active::remaining_time_estimate() const
{
  // A pause hold move will last indefinitely until it gets cancelled, which
  // may happen at any time (resume, or an auto-cancel timeout).
  return rmf_traffic::Duration(0);
}

//==============================================================================
auto PauseHoldMove::Active::backup() const -> Backup
{
  // PauseHoldMove doesn't need to be backed up
  return Backup::make(0, nlohmann::json());
}

//==============================================================================
//called when the robot's whole task needs to pause for something else. Stops the robot, gives back a resume callback.
auto PauseHoldMove::Active::interrupt(
  std::function<void()> task_is_interrupted) -> Resume
{
  _negotiator->clear_license();
  _is_interrupted = true;
  _stop_and_clear();

  _state->update_status(Status::Standby);
  _state->update_log().info("Going into standby for an interruption");
  _state->update_dependencies({});

  _context->worker().schedule(
    [task_is_interrupted](const auto&)
    {
      task_is_interrupted();
    });

  return Resume::make(
    [w = weak_from_this()]()
    {
      if (const auto self = w.lock())
      {
        self->_negotiator->claim_license();
        self->_is_interrupted = false;
        self->_find_plan();
      }
    });
}

//==============================================================================
// called on resume or auto-cancel timeout. Stops the robot and finishes the event for good.
void PauseHoldMove::Active::cancel()
{
  RCLCPP_INFO(
    _context->node()->get_logger(),
    "Canceling pause hold move for robot [%s]",
    _context->requester_id().c_str());
  _stop_and_clear();
  _state->update_status(Status::Canceled);
  _state->update_log().info("Received signal to cancel");
  _finished();
}

//==============================================================================
//same as cancel, for a hard kill.
void PauseHoldMove::Active::kill()
{
  _stop_and_clear();
  _state->update_status(Status::Killed);
  _state->update_log().info("Received signal to kill");
  _finished();
}

//==============================================================================
// asks the RMF planner for a path to the charger.
void PauseHoldMove::Active::_find_plan()
{
  if (_is_interrupted)
    return;

  _state->update_status(Status::Underway);
  _state->update_log().info("Searching for a plan to the hold point");

  _find_path_service = std::make_shared<services::FindPath>(
    _context->planner(), _context->location(), _goal,
    _context->schedule()->snapshot(), _context->itinerary().id(),
    _context->profile(),
    std::chrono::seconds(5));

  const auto start_name = wp_name(*_context);
  const auto goal_name = wp_name(*_context, _goal);

  _plan_subscription = rmf_rxcpp::make_job<services::FindPath::Result>(
    _find_path_service)
    .observe_on(rxcpp::identity_same_worker(_context->worker()))
    .subscribe(
    [w = weak_from_this(), start_name, goal_name](
      const services::FindPath::Result& result)
    {
      const auto self = w.lock();
      if (!self)
        return;

      if (!result)
      {
        // The planner could not find a way to reach the goal
        self->_state->update_status(Status::Error);
        self->_state->update_log().error(
          "Failed to find a plan to move from ["
          + start_name + "] to [" + goal_name + "]. Will retry soon.");

        self->_execution = std::nullopt;
        self->_schedule_retry();

        self->_context->worker()
        .schedule([update = self->_update](const auto&) { update(); });

        return;
      }

      self->_state->update_status(Status::Underway);
      self->_state->update_log().info(
        "Found a plan to move from ["
        + start_name + "] to [" + goal_name + "]");

      auto full_itinerary = project_itinerary(
        *result, {},
        *self->_context->planner());

      self->_execute_plan(
        self->_context->itinerary().assign_plan_id(),
        *std::move(result),
        std::move(full_itinerary));

      self->_find_path_service = nullptr;
      self->_retry_timer = nullptr;
    });

  _find_path_timeout = _context->node()->try_create_wall_timer(
    std::chrono::seconds(10),
    [
      weak_service = _find_path_service->weak_from_this(),
      weak_self = weak_from_this()
    ]()
    {
      if (const auto service = weak_service.lock())
        service->interrupt();

      if (const auto self = weak_self.lock())
        self->_find_path_timeout = nullptr;
    });

  _update();
}

//==============================================================================
//: if planning fails, tries again in 5 seconds.
void PauseHoldMove::Active::_schedule_retry()
{
  if (_retry_timer)
    return;

  _retry_timer = _context->node()->try_create_wall_timer(
    std::chrono::seconds(5),
    [w = weak_from_this()]()
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_retry_timer = nullptr;
      if (self->_execution.has_value())
        return;

      self->_find_plan();
    });
}

//==============================================================================
//once a path is found, drives the robot there. If already at the charger, skips straight to holding.
void PauseHoldMove::Active::_execute_plan(
  const rmf_traffic::PlanId plan_id,
  rmf_traffic::agv::Plan plan,
  rmf_traffic::schedule::Itinerary full_itinerary)
{
  if (_is_interrupted)
    return;

  if (plan.get_itinerary().empty() || plan.get_waypoints().empty())
  {
    // Already at the hold point. Unlike EmergencyPullover, we do NOT call
    // _finished(). here we hold in place until an explicit cancel()/kill(). this is the one different with EmergencyPullover
    RCLCPP_INFO(
      _context->node()->get_logger(),
      "Robot [%s] is already at its hold point",
      _context->requester_id().c_str());
    _on_arrived();
    return;
  }

  if (!plan.get_waypoints().back().graph_index().has_value())
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Robot [%s] has no graph index for its final waypoint. This is a serious "
      "bug and should be reported to the RMF maintainers.",
      _context->requester_id().c_str());
    _schedule_retry();
    return;
  }

  _execution = ExecutePlan::make(
    _context, plan_id, std::move(plan), _goal,
    std::move(full_itinerary), _assign_id, _state, _update,
    [w = weak_from_this()]()
    {
      if (const auto self = w.lock())
        self->_on_arrived();
    },
    std::nullopt);

  if (!_execution.has_value())
  {
    _state->update_status(Status::Error);
    _state->update_log().error(
      "Invalid (empty) plan generated. Will retry soon. "
      "Please report this incident to the Open-RMF developers.");
    _schedule_retry();
  }
}

//==============================================================================
//: marks that the robot reached the charger, now just holds instead of finishing.
void PauseHoldMove::Active::_on_arrived()
{
  // Arriving at the hold point does NOT finish this event -- only an
  // explicit cancel()/kill() (e.g. resume, or an auto-cancel timeout) does.
  _is_holding = true;
  _state->update_status(Status::Underway);
  _state->update_log().info(
    "Arrived at hold point; holding until told to stop");
  _update();
}

//==============================================================================
//: stops the robot's current command and clears its spot in the traffic schedule.
void PauseHoldMove::Active::_stop_and_clear()
{
  _execution = std::nullopt;
  _is_holding = false;
  if (const auto command = _context->command())
    command->stop();

  if (_retry_timer)
    _retry_timer->cancel();
  _context->itinerary().clear();
}

//==============================================================================
//: answers other robots' traffic negotiation requests when they cross paths with this one.
Negotiator::NegotiatePtr PauseHoldMove::Active::_respond(
  const Negotiator::TableViewerPtr& table_view,
  const Negotiator::ResponderPtr& responder)
{
  auto approval_cb = [w = weak_from_this()](
    const rmf_traffic::PlanId plan_id,
    const rmf_traffic::agv::Plan& plan,
    rmf_traffic::schedule::Itinerary full_itinerary)
    -> std::optional<rmf_traffic::schedule::ItineraryVersion>
    {
      if (auto self = w.lock())
      {
        self->_execute_plan(plan_id, plan, std::move(full_itinerary));
        return self->_context->itinerary().version();
      }

      return std::nullopt;
    };

  const auto evaluator = Negotiator::make_evaluator(table_view);
  return services::Negotiate::path(
    _context->itinerary().assign_plan_id(), _context->planner(),
    _context->location(), _goal, {}, table_view,
    responder, std::move(approval_cb), std::move(evaluator));
}

//==============================================================================
//:wires this event class into RMF's task system so it can be scheduled at all.
rmf_task::Activator PauseHoldMove::_make_activator(
  std::function<rmf_traffic::Time()> clock)
{
  auto event_activator =
    std::make_shared<rmf_task_sequence::Event::Initializer>();
  event_activator->add<PauseHoldMoveDescription>(
    [](
      const AssignIDPtr& id,
      const std::function<rmf_task::State()>& get_state,
      const rmf_task::ConstParametersPtr&,
      const PauseHoldMoveDescription& description,
      std::function<void()> update) -> rmf_task_sequence::Event::StandbyPtr
    {
      return PauseHoldMove::Standby::make(
        id, get_state().get<agv::GetContext>()->value,
        description.goal(), std::move(update));
    },
    [](
      const AssignIDPtr& id,
      const std::function<rmf_task::State()>& get_state,
      const rmf_task::ConstParametersPtr&,
      const PauseHoldMoveDescription& description,
      const nlohmann::json&,
      std::function<void()> update,
      std::function<void()> checkpoint,
      std::function<void()> finished) -> rmf_task_sequence::Event::ActivePtr
    {
      return PauseHoldMove::Standby::make(
        id, get_state().get<agv::GetContext>()->value,
        description.goal(), std::move(update))
      ->begin(std::move(checkpoint), std::move(finished));
    });

  auto phase_activator =
    std::make_shared<rmf_task_sequence::Phase::Activator>();
  rmf_task_sequence::phases::SimplePhase::add(
    *phase_activator, event_activator);

  rmf_task::Activator activator;
  rmf_task_sequence::Task::add(activator, phase_activator, std::move(clock));
  return activator;
}

} // namespace events
} // namespace rmf_fleet_adapter
