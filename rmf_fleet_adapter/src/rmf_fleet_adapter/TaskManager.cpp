/*
 * Copyright (C) 2020 Open Source Robotics Foundation
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

#include "TaskManager.hpp"
#include "log_to_json.hpp"

#include <rmf_task/requests/ChargeBattery.hpp>
#include <rmf_task/requests/Clean.hpp>
#include <rmf_task/requests/Delivery.hpp>
#include <rmf_task/requests/Loop.hpp>

#include <rmf_traffic_ros2/Time.hpp>

#include <rmf_traffic/agv/Planner.hpp>

#include "agv/internal_FleetUpdateHandle.hpp"
#include "tasks/Clean.hpp"
#include "tasks/ChargeBattery.hpp"
#include "tasks/Delivery.hpp"
#include "tasks/Patrol.hpp"

#include "events/ResponsiveWait.hpp"
#include "events/EmergencyPullover.hpp"

#include <rmf_api_msgs/schemas/task_state_update.hpp>
#include <rmf_api_msgs/schemas/task_state.hpp>
#include <rmf_api_msgs/schemas/task_log_update.hpp>
#include <rmf_api_msgs/schemas/task_log.hpp>
#include <rmf_api_msgs/schemas/log_entry.hpp>
#include <rmf_api_msgs/schemas/simple_response.hpp>
#include <rmf_api_msgs/schemas/token_response.hpp>
#include <rmf_api_msgs/schemas/cancel_task_request.hpp>
#include <rmf_api_msgs/schemas/cancel_task_response.hpp>
#include <rmf_api_msgs/schemas/kill_task_request.hpp>
#include <rmf_api_msgs/schemas/kill_task_response.hpp>
#include <rmf_api_msgs/schemas/interrupt_task_request.hpp>
#include <rmf_api_msgs/schemas/interrupt_task_response.hpp>
#include <rmf_api_msgs/schemas/resume_task_request.hpp>
#include <rmf_api_msgs/schemas/resume_task_response.hpp>
#include <rmf_api_msgs/schemas/rewind_task_request.hpp>
#include <rmf_api_msgs/schemas/rewind_task_response.hpp>
#include <rmf_api_msgs/schemas/robot_task_request.hpp>
#include <rmf_api_msgs/schemas/dispatch_task_response.hpp>
#include <rmf_api_msgs/schemas/task_state.hpp>
#include <rmf_api_msgs/schemas/error.hpp>
#include <rmf_api_msgs/schemas/robot_task_response.hpp>
#include <rmf_api_msgs/schemas/skip_phase_request.hpp>
#include <rmf_api_msgs/schemas/skip_phase_response.hpp>
#include <rmf_api_msgs/schemas/task_request.hpp>
#include <rmf_api_msgs/schemas/undo_skip_phase_request.hpp>
#include <rmf_api_msgs/schemas/undo_skip_phase_response.hpp>
#include <rmf_api_msgs/schemas/error.hpp>
#include <rmf_api_msgs/schemas/commission.hpp>
#include <rmf_api_msgs/schemas/robot_commission_request.hpp>
#include <rmf_api_msgs/schemas/robot_commission_response.hpp>

namespace rmf_fleet_adapter {

//==============================================================================
TaskManagerPtr TaskManager::make(
  agv::RobotContextPtr context,
  std::optional<std::weak_ptr<rmf_websocket::BroadcastClient>> broadcast_client,
  std::weak_ptr<agv::FleetUpdateHandle> fleet_handle)
{
  std::shared_ptr<agv::FleetUpdateHandle> fleet = fleet_handle.lock();
  if (!fleet)
    return nullptr;

  auto mgr = TaskManagerPtr(
    new TaskManager(
      std::move(context),
      std::move(broadcast_client),
      std::move(fleet_handle)));

  auto begin_pullover = [w = mgr->weak_from_this()]()
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_context->worker().schedule(
        [w = self->weak_from_this()](const auto&)
        {
          const auto self = w.lock();
          if (self && self->_emergency_active)
            self->_begin_pullover();
        });
    };

  mgr->_emergency_sub = agv::FleetUpdateHandle::Implementation::get(*fleet)
    .emergency_obs
    .observe_on(rxcpp::identity_same_worker(mgr->_context->worker()))
    .subscribe(
    [w = mgr->weak_from_this(), begin_pullover](const bool is_emergency)
    {
      if (auto mgr = w.lock())
      {
        if (mgr->_emergency_active == is_emergency)
          return;

        mgr->_emergency_active = is_emergency;
        if (is_emergency)
        {
          if (mgr->_waiting)
          {
            // Cancel any waiting behavior that might be active.
            mgr->_waiting.cancel({"emergency pullover"}, mgr->_context->now());
          }

          if (mgr->_active_task)
          {
            mgr->_emergency_pullover_interrupt_token =
            mgr->_active_task.add_interruption(
              {"emergency pullover"},
              mgr->_context->now(),
              [begin_pullover]() { begin_pullover(); });
          }
          else
          {
            begin_pullover();
          }
        }
        else
        {
          if (mgr->_emergency_pullover)
          {
            if (mgr->_emergency_pullover.is_finished())
            {
              mgr->_resume_from_emergency();
            }
            else
            {
              mgr->_emergency_pullover.cancel(
                {"emergency notice topic"}, mgr->_context->now());
            }
          }
        }
      }
    });

  mgr->_task_timer = mgr->context()->node()->try_create_wall_timer(
    std::chrono::seconds(1),
    [w = mgr->weak_from_this()]()
    {
      if (auto mgr = w.lock())
      {
        mgr->_begin_next_task();
      }
    });

  mgr->_begin_waiting();

  // TODO(MXG): The tests allow a task manager to be created before a task
  // planner is available. To deal with that we'll skip making the
  // travel_estimator here. This code is unsound and the flow of object
  // construction and inter-dependencies should be improved.
  if (mgr->_context->task_planner())
  {
    mgr->_travel_estimator = std::make_shared<rmf_task::TravelEstimator>(
      mgr->_context->task_planner()->configuration().parameters());
  }

  mgr->_update_timer = mgr->_context->node()->try_create_wall_timer(
    std::chrono::milliseconds(100),
    [w = mgr->weak_from_this()]()
    {
      if (const auto self = w.lock())
        self->_consider_publishing_updates();
    });

  mgr->_task_request_api_sub = mgr->_context->node()->task_api_request()
    .observe_on(rxcpp::identity_same_worker(mgr->_context->worker()))
    .subscribe(
    [w = mgr->weak_from_this()](
      const rmf_task_msgs::msg::ApiRequest::SharedPtr& request)
    {
      if (const auto self = w.lock())
        self->_handle_request(request->json_msg, request->request_id);
    });

  const std::vector<nlohmann::json> schemas = {
    rmf_api_msgs::schemas::task_state,
    rmf_api_msgs::schemas::task_log,
    rmf_api_msgs::schemas::log_entry,
    rmf_api_msgs::schemas::task_state_update,
    rmf_api_msgs::schemas::task_log_update,
    rmf_api_msgs::schemas::simple_response,
    rmf_api_msgs::schemas::token_response,
    rmf_api_msgs::schemas::cancel_task_request,
    rmf_api_msgs::schemas::cancel_task_response,
    rmf_api_msgs::schemas::kill_task_request,
    rmf_api_msgs::schemas::kill_task_response,
    rmf_api_msgs::schemas::interrupt_task_request,
    rmf_api_msgs::schemas::interrupt_task_response,
    rmf_api_msgs::schemas::resume_task_request,
    rmf_api_msgs::schemas::resume_task_response,
    rmf_api_msgs::schemas::rewind_task_request,
    rmf_api_msgs::schemas::rewind_task_response,
    rmf_api_msgs::schemas::robot_task_request,
    rmf_api_msgs::schemas::dispatch_task_response,
    rmf_api_msgs::schemas::error,
    rmf_api_msgs::schemas::robot_task_response,
    rmf_api_msgs::schemas::skip_phase_request,
    rmf_api_msgs::schemas::skip_phase_response,
    rmf_api_msgs::schemas::task_request,
    rmf_api_msgs::schemas::undo_skip_phase_request,
    rmf_api_msgs::schemas::undo_skip_phase_response,
    rmf_api_msgs::schemas::commission,
    rmf_api_msgs::schemas::robot_commission_request,
    rmf_api_msgs::schemas::robot_commission_response,
  };

  for (const auto& schema : schemas)
  {
    const auto json_uri = nlohmann::json_uri{schema["$id"]};
    mgr->_schema_dictionary.insert({json_uri.url(), schema});
  }

  mgr->_context->_set_task_manager(mgr);

  return mgr;
}

//==============================================================================
TaskManager::TaskManager(
  agv::RobotContextPtr context,
  std::optional<std::weak_ptr<rmf_websocket::BroadcastClient>> broadcast_client,
  std::weak_ptr<agv::FleetUpdateHandle> fleet_handle)
: _context(std::move(context)),
  _broadcast_client(std::move(broadcast_client)),
  _fleet_handle(std::move(fleet_handle)),
  _next_sequence_number(0),
  _last_update_time(std::chrono::steady_clock::now() - std::chrono::seconds(1))
{
  // Do nothing. The make() function does all further initialization.
}

//==============================================================================
TaskManager::ActiveTask::ActiveTask()
: _task(nullptr),
  _interruption_handler(std::make_shared<InterruptionHandler>())
{
  // Do nothing
}

//==============================================================================
const std::string& TaskManager::ActiveTask::id() const
{
  if (!_task)
  {
    /* *INDENT-OFF* */
    throw std::runtime_error(
      "[TaskManager::ActiveTask::id] Called when there is no active task. "
      "This is a serious bug, please report this to the developers of RMF ");
    /* *INDENT-ON* */
  }

  return _task->tag()->booking()->id();
}

namespace {

//==============================================================================
std::string status_to_string(rmf_task::Event::Status status)
{
  using Status = rmf_task::Event::Status;
  switch (status)
  {
    case Status::Uninitialized:
      return "uninitialized";
    case Status::Blocked:
      return "blocked";
    case Status::Error:
      return "error";
    case Status::Failed:
      return "failed";
    case Status::Standby:
      return "standby";
    case Status::Underway:
      return "underway";
    case Status::Delayed:
      return "delayed";
    case Status::Skipped:
      return "skipped";
    case Status::Canceled:
      return "canceled";
    case Status::Killed:
      return "killed";
    case Status::Completed:
      return "completed";
    default:
      return "uninitialized";
  }
}

//==============================================================================
nlohmann::json& copy_phase_data(
  nlohmann::json& phases,
  const rmf_task::Phase::Active& snapshot,
  rmf_task::Log::Reader& reader,
  nlohmann::json& all_phase_logs,
  bool quiet_cancel = false)
{
  const auto& tag = *snapshot.tag();
  const auto& header = tag.header();
  const auto id = tag.id();
  auto& phase_state = phases[std::to_string(id)];
  phase_state["id"] = id;
  phase_state["category"] = header.category();
  phase_state["detail"] = header.detail();
  phase_state["original_estimate_millis"] =
    std::max(0l, to_millis(header.original_duration_estimate()).count());
  phase_state["estimate_millis"] =
    std::max(0l, to_millis(snapshot.estimate_remaining_time()).count());
  phase_state["final_event_id"] = snapshot.final_event()->id();
  //auto& event_states = phase_state["events"];

  // TODO(MXG): Add in skip request information

  std::vector<rmf_task::Event::ConstStatePtr> event_queue;
  event_queue.push_back(snapshot.final_event());

  auto& phase_logs = all_phase_logs[std::to_string(id)];
  auto& event_logs = phase_logs["events"];
  event_logs = std::unordered_map<std::string, std::vector<nlohmann::json>>();

  /*
  while (!event_queue.empty())
  {
    const auto top = event_queue.back();
    event_queue.pop_back();

    auto& event_state = event_states[std::to_string(top->id())];
    event_state["id"] = top->id();
    event_state["status"] = status_to_string(top->status());
    if (quiet_cancel && top->status() == rmf_task::Event::Status::Canceled)
    {
      event_state["status"] = status_to_string(
        rmf_task::Event::Status::Completed);
    }

    // TODO(MXG): Keep a VersionedString Reader to know when to actually update
    // this string
    event_state["name"] =
      *rmf_task::VersionedString::Reader().read(top->name());

    event_state["detail"] =
      *rmf_task::VersionedString::Reader().read(top->detail());

    std::vector<nlohmann::json> logs;
    for (const auto& log : reader.read(top->log()))
      logs.push_back(log_to_json(log));

    if (!logs.empty())
      event_logs[std::to_string(top->id())] = std::move(logs);

    std::vector<uint32_t> deps;
    deps.reserve(top->dependencies().size());
    for (const auto& dep : top->dependencies())
    {
      event_queue.push_back(dep);
      deps.push_back(dep->id());
    }

    event_state["deps"] = std::move(deps);
  }
  */

  return phase_state;
}

//==============================================================================
void copy_phase_data(
  nlohmann::json& phases,
  const rmf_task::Phase::Pending& pending)
{
  const auto id = pending.tag()->id();
  auto& phase = phases[std::to_string(id)];
  phase["id"] = id;

  const auto& header = pending.tag()->header();
  phase["category"] = header.category();
  phase["detail"] = header.detail();
  phase["estimate_millis"] =
    std::max(0l, to_millis(header.original_duration_estimate()).count());
}

//==============================================================================
void copy_booking_data(
  nlohmann::json& booking_json,
  const rmf_task::Task::Booking& booking)
{
  booking_json["id"] = booking.id();
  booking_json["unix_millis_earliest_start_time"] =
    to_millis(booking.earliest_start_time().time_since_epoch()).count();
  const auto requester = booking.requester();
  if (requester.has_value())
  {
    booking_json["requester"] = requester.value();
  }
  const auto request_time = booking.request_time();
  if (request_time.has_value())
  {
    booking_json["unix_millis_request_time"] =
      to_millis(request_time.value().time_since_epoch()).count();
  }
  const auto labels = booking.labels();
  if (labels.size() != 0)
  {
    booking_json["labels"] = booking.labels();
  }
  // TODO(MXG): Add priority
}

//==============================================================================
void copy_assignment(
  nlohmann::json& assigned_to_json,
  const agv::RobotContext& context)
{
  assigned_to_json["group"] = context.group();
  assigned_to_json["name"] = context.name();
}

//==============================================================================
nlohmann::json make_simple_success_response()
{
  nlohmann::json response;
  response["success"] = true;
  return response;
}

} // anonymous namespace

//==============================================================================
TaskManager::ActiveTask TaskManager::ActiveTask::start(
  rmf_task::Task::ActivePtr task,
  rmf_traffic::Time time)
{
  ActiveTask new_task;
  new_task._task = std::move(task);
  new_task._start_time = time;

  return new_task;
}

//==============================================================================
void TaskManager::ActiveTask::publish_task_state(TaskManager& mgr)
{
  auto task_state_update = mgr._task_state_update_json;

  const auto& booking = *_task->tag()->booking();
  copy_booking_data(_state_msg["booking"], booking);
  const auto& header = _task->tag()->header();
  _state_msg["category"] = header.category();
  _state_msg["detail"] = header.detail();

  const auto remaining_time_estimate = _task->estimate_remaining_time();
  const auto finish_estimate = mgr.context()->now()+remaining_time_estimate;
  _state_msg["unix_millis_start_time"] =
    to_millis(_start_time.time_since_epoch()).count();
  _state_msg["unix_millis_finish_time"] =
    to_millis(finish_estimate.time_since_epoch()).count();
  _state_msg["original_estimate_millis"] =
    std::max(0l, to_millis(header.original_duration_estimate()).count());
  _state_msg["estimate_millis"] =
    std::max(0l, to_millis(remaining_time_estimate).count());
  copy_assignment(_state_msg["assigned_to"], *mgr._context);
  _state_msg["status"] =
    status_to_string(_task->status_overview());
  auto& phases = _state_msg["phases"];

  nlohmann::json task_logs;
  task_logs["task_id"] = booking.id();
  auto& phase_logs = task_logs["phases"];

  std::vector<uint64_t> completed_ids;
  completed_ids.reserve(_task->completed_phases().size());
  for (const auto& completed : _task->completed_phases())
  {
    const auto& snapshot = completed->snapshot();
    auto& phase = copy_phase_data(
      phases, *snapshot, mgr._log_reader, phase_logs, _quiet_cancel);
    phase["unix_millis_start_time"] =
      to_millis(completed->start_time().time_since_epoch()).count();

    phase["unix_millis_finish_time"] =
      to_millis(completed->finish_time().time_since_epoch()).count();

    completed_ids.push_back(snapshot->tag()->id());
  }
  _state_msg["completed"] = std::move(completed_ids);

  const auto active_phase = _task->active_phase();
  if (active_phase == nullptr)
    return;
  auto& active =
    copy_phase_data(
    phases, *active_phase, mgr._log_reader, phase_logs, _quiet_cancel);
  if (_task->active_phase_start_time().has_value())
  {
    active["unix_millis_start_time"] =
      to_millis(_task->active_phase_start_time()->time_since_epoch()).count();
  }
  _state_msg["active"] = active_phase->tag()->id();

  std::vector<uint64_t> pending_ids;
  pending_ids.reserve(_task->pending_phases().size());
  for (const auto& pending : _task->pending_phases())
  {
    copy_phase_data(phases, pending);
    pending_ids.push_back(pending.tag()->id());
  }
  _state_msg["pending"] = std::move(pending_ids);

  if (!_active_interruptions.empty() || !_removed_interruptions.empty())
  {
    auto& interruptions_json = _state_msg["interruptions"];
    for (const auto& i : {&_active_interruptions, &_removed_interruptions})
    {
      for (const auto& [token, msg] : *i)
        interruptions_json[token] = msg;
    }
  }

  if (_cancellation.has_value() && !_quiet_cancel)
    _state_msg["cancellation"] = *_cancellation;

  if (_killed.has_value())
    _state_msg["killed"] = *_killed;

  for (const auto& [phase, skip_info] : _skip_info_map)
  {
    auto& skip_requests = phases[phase]["skip_requests"];
    for (const auto& s : {&skip_info.active_skips, &skip_info.removed_skips})
    {
      for (const auto& [token, msg] : *s)
        skip_requests[token] = msg;
    }
  }

  if (_quiet_cancel &&
    _task->status_overview() == rmf_task::Event::Status::Canceled)
  {
    _state_msg["status"] = status_to_string(rmf_task::Event::Status::Completed);
  }

  task_state_update["data"] = _state_msg;

  static const auto task_update_validator =
    mgr._make_validator(rmf_api_msgs::schemas::task_state_update);
  mgr._validate_and_publish_websocket(task_state_update, task_update_validator);

  auto task_log_update = nlohmann::json();
  task_log_update["type"] = "task_log_update";
  task_log_update["data"] = task_logs;

  static const auto log_update_validator =
    mgr._make_validator(rmf_api_msgs::schemas::task_log_update);
  mgr._validate_and_publish_websocket(task_log_update, log_update_validator);
}

//==============================================================================
std::string TaskManager::ActiveTask::add_interruption(
  std::vector<std::string> labels,
  rmf_traffic::Time time,
  std::function<void()> task_is_interrupted)
{
  std::string token = std::to_string(_next_token++);

  nlohmann::json interruption_json;
  interruption_json["unix_millis_request_time"] =
    to_millis(time.time_since_epoch()).count();

  interruption_json["labels"] = std::move(labels);

  _active_interruptions[token] = std::move(interruption_json);

  if (_resume_task.has_value())
  {
    std::lock_guard<std::mutex> lock(_interruption_handler->mutex);
    if (_interruption_handler->is_interrupted)
    {
      task_is_interrupted();
    }
    else
    {
      _interruption_handler->interruption_listeners
      .push_back(std::move(task_is_interrupted));
    }

    return token;
  }

  _interruption_handler->interruption_listeners
  .push_back(std::move(task_is_interrupted));

  _resume_task = _task->interrupt(
    [w = _interruption_handler->weak_from_this()]()
    {
      const auto handler = w.lock();
      if (!handler)
        return;

      std::lock_guard<std::mutex> lock(handler->mutex);
      handler->is_interrupted = true;
      for (const auto& listener : handler->interruption_listeners)
      {
        listener();
      }
    });

  return token;
}

//==============================================================================
bool TaskManager::ActiveTask::is_interrupted() const
{
  return _interruption_handler->is_interrupted;
}

//==============================================================================
bool TaskManager::ActiveTask::is_finished() const
{
  if (_task)
    return _task->finished();

  return true;
}

//==============================================================================
std::vector<std::string> TaskManager::ActiveTask::remove_interruption(
  std::vector<std::string> for_tokens,
  std::vector<std::string> labels,
  rmf_traffic::Time time)
{
  nlohmann::json resume_json;
  resume_json["unix_millis_resume_time"] =
    to_millis(time.time_since_epoch()).count();

  resume_json["labels"] = std::move(labels);

  std::vector<std::string> missing_tokens;
  for (const auto& token : for_tokens)
  {
    const auto it = _active_interruptions.find(token);
    if (it == _active_interruptions.end())
    {
      if (_removed_interruptions.count(token) == 0)
      {
        missing_tokens.push_back(token);
      }
      continue;
    }

    auto interruption_json = it->second;
    interruption_json["resumed_by"] = resume_json;
    _removed_interruptions[token] = interruption_json;
    _active_interruptions.erase(it);
  }

  if (_active_interruptions.empty())
  {
    if (_resume_task.has_value())
    {
      std::lock_guard<std::mutex> lock(_interruption_handler->mutex);
      _interruption_handler->is_interrupted = false;
      _interruption_handler->interruption_listeners.clear();

      (*_resume_task)();
      _resume_task = std::nullopt;
    }
  }

  return missing_tokens;
}

//==============================================================================
void TaskManager::ActiveTask::cancel(
  std::vector<std::string> labels,
  rmf_traffic::Time time)
{
  if (_cancellation.has_value())
    return;

  nlohmann::json cancellation;
  cancellation["unix_millis_request_time"] =
    to_millis(time.time_since_epoch()).count();

  cancellation["labels"] = std::move(labels);

  _cancellation = std::move(cancellation);
  _task->cancel();
}

//==============================================================================
void TaskManager::ActiveTask::kill(
  std::vector<std::string> labels,
  rmf_traffic::Time time)
{
  if (_killed.has_value())
    return;

  nlohmann::json killed;
  killed["unix_millis_request_time"] =
    to_millis(time.time_since_epoch()).count();

  killed["labels"] = std::move(labels);

  _killed = std::move(killed);
  _task->kill();
}

//==============================================================================
void TaskManager::ActiveTask::rewind(uint64_t phase_id)
{
  _task->rewind(phase_id);
}

//==============================================================================
void TaskManager::ActiveTask::quiet_cancel(
  std::vector<std::string> labels,
  rmf_traffic::Time time)
{
  if (_cancellation.has_value())
    return;

  nlohmann::json cancellation;
  cancellation["unix_millis_request_time"] =
    to_millis(time.time_since_epoch()).count();

  cancellation["labels"] = std::move(labels);

  _cancellation = std::move(cancellation);
  _quiet_cancel = true;
  _task->cancel();
}

//==============================================================================
std::string TaskManager::ActiveTask::skip(
  uint64_t phase_id,
  std::vector<std::string> labels,
  rmf_traffic::Time time)
{
  std::string token = std::to_string(_next_token++);

  auto& skip_info = _skip_info_map[phase_id];

  nlohmann::json skip_json;
  skip_json["unix_millis_request_time"] =
    to_millis(time.time_since_epoch()).count();

  skip_json["labels"] = std::move(labels);

  skip_info.active_skips[token] = std::move(skip_json);

  _task->skip(phase_id, true);

  return token;
}

//==============================================================================
std::vector<std::string> TaskManager::ActiveTask::remove_skips(
  const std::vector<std::string>& for_tokens,
  std::vector<std::string> labels,
  rmf_traffic::Time time)
{
  nlohmann::json undo_json;
  undo_json["unix_millis_request_time"] =
    to_millis(time.time_since_epoch()).count();

  undo_json["labels"] = std::move(labels);

  std::vector<std::string> missing_tokens;
  for (const auto& token : for_tokens)
  {
    bool found_token = false;
    for (auto& [phase, skip_info] : _skip_info_map)
    {
      const auto it = skip_info.active_skips.find(token);
      if (it == skip_info.active_skips.end())
        continue;

      auto skip_json = it->second;
      skip_json["undo"] = undo_json;
      skip_info.removed_skips[token] = std::move(skip_json);
      skip_info.active_skips.erase(it);

      if (skip_info.active_skips.empty())
        _task->skip(phase, false);

      found_token = true;
      break;
    }

    if (!found_token)
      missing_tokens.push_back(token);
  }

  return missing_tokens;
}

//==============================================================================
std::optional<std::string> TaskManager::current_task_id() const
{
  if (_active_task)
    return _active_task.id();

  return std::nullopt;
}

//==============================================================================
auto TaskManager::get_queue() const -> const std::vector<Assignment>&
{
  return _queue;
}

//==============================================================================
bool TaskManager::cancel_task_if_present(const std::string& task_id)
{
  if (_active_task && _active_task.id() == task_id)
  {
    _active_task.cancel({"DispatchRequest"}, _context->now());
    return true;
  }

  return _cancel_task_from_dispatch_queue(task_id, {"DispatchRequest"});
}

//==============================================================================
std::string TaskManager::robot_status() const
{
  if (_context->override_status().has_value())
    return _context->override_status().value();

  if (_context->is_charging())
  {
    if (_context->waiting_for_charger())
    {
      return "idle";
    }
    else
    {
      return "charging";
    }
  }

  if (!_active_task)
    return "idle";

  return "working";
}

//==============================================================================
auto TaskManager::expected_finish_state() const -> State
{
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  if (!_direct_queue.empty())
  {
    return _direct_queue.rbegin()->assignment.finish_state();
  }

  if (_active_task)
    return _context->current_task_end_state();

  rmf_task::State current_state =
    _context->make_get_state()()
    .time(rmf_traffic_ros2::convert(_context->node()->now()));
  return current_state;
}

//==============================================================================
const agv::RobotContextPtr& TaskManager::context()
{
  return _context;
}

//==============================================================================
agv::ConstRobotContextPtr TaskManager::context() const
{
  return _context;
}

//==============================================================================
std::optional<std::weak_ptr<rmf_websocket::BroadcastClient>> TaskManager::
broadcast_client()
const
{
  return _broadcast_client;
}

//==============================================================================
void TaskManager::enable_responsive_wait(bool value)
{
  if (_responsive_wait_enabled == value)
    return;

  _responsive_wait_enabled = value;
  if (!_responsive_wait_enabled && static_cast<bool>(_waiting))
  {
    _waiting.cancel({"Responsive Wait Disabled"}, _context->now());
    return;
  }

  if (_responsive_wait_enabled)
  {
    std::lock_guard<std::recursive_mutex> guard(_mutex);
    if (!_active_task && _queue.empty() && _direct_queue.empty() && !_waiting)
    {
      _begin_waiting();
    }
  }
}

//==============================================================================
void TaskManager::set_idle_task(rmf_task::ConstRequestFactoryPtr task)
{
  if (_idle_task == task)
    return;

  _idle_task = std::move(task);
  std::lock_guard<std::recursive_mutex> guard(_mutex);
  if (!_active_task && _queue.empty() && _direct_queue.empty())
  {
    _begin_waiting();
  }
}

//==============================================================================
void TaskManager::set_queue(
  const std::vector<TaskManager::Assignment>& assignments)
{
  // We indent this block as _mutex is also locked in the _begin_next_task()
  // function that is called at the end of this function.
  {
    std::lock_guard<std::recursive_mutex> guard(_mutex);
    // Do not remove automatic task if assignments is empty. See Issue #138
    if (assignments.empty() &&
      _queue.size() == 1 &&
      _queue.front().request()->booking()->automatic())
    {
      return;
    }

    // Cancel pending automatic tasks that will be removed from the new
    // assignments.
    std::unordered_set<std::string> new_automatic_task_ids;
    const std::vector<std::string> cancellation_labels =
    {"New task assignments received."};
    for (const auto& a : assignments)
    {
      if (a.request()->booking()->automatic())
      {
        new_automatic_task_ids.insert(a.request()->booking()->id());
      }
    }
    for (const auto& a : _queue)
    {
      if (a.request()->booking()->automatic() &&
        new_automatic_task_ids.find(a.request()->booking()->id()) ==
        new_automatic_task_ids.end())
      {
        _publish_canceled_pending_task(a, cancellation_labels);
      }
    }

    _queue = assignments;
    _publish_task_queue();
  }

  _begin_next_task();
}

//==============================================================================
std::vector<rmf_task::ConstRequestPtr> TaskManager::dispatched_requests() const
{
  using namespace rmf_task::requests;
  std::vector<rmf_task::ConstRequestPtr> requests;
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  requests.reserve(_queue.size());
  for (const auto& task : _queue)
  {
    if (task.request()->booking()->automatic())
    {
      continue;
    }

    requests.push_back(task.request());
  }
  return requests;
}

//==============================================================================
void TaskManager::reassign_dispatched_requests(
  std::function<void()> on_success,
  std::function<void(std::vector<std::string>)> on_failure)
{
  std::vector<Assignment> assignments = _drain_dispatched_assignments();
  const auto fleet = _fleet_handle.lock();
  if (!fleet)
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Attempting to reassign tasks for [%s] but its fleet is shutting down",
      _context->requester_id().c_str());
    return;
  }

  auto& fleet_impl = agv::FleetUpdateHandle::Implementation::get(*fleet);
  auto& unassigned = fleet_impl.unassigned_requests;
  for (const auto& a : assignments)
  {
    unassigned.push_back(a.request());
  }

  fleet_impl.reassign_dispatched_tasks(
    [name = _context->requester_id(), node = _context->node(), on_success]()
    {
      RCLCPP_INFO(
        node->get_logger(),
        "Successfully reassigned tasks for [%s]",
        name.c_str());
      on_success();
    },
    [
      name = _context->requester_id(),
      node = _context->node(),
      assignments,
      fleet,
      on_failure,
      self = shared_from_this()
    ](std::vector<std::string> errors)
    {
      std::stringstream ss_errors;
      if (errors.empty())
      {
        ss_errors << " no reason given";
      }
      else
      {
        for (auto&& e : errors)
        {
          ss_errors << "\n -- " << e;
        }
      }

      std::stringstream ss_requests;
      if (assignments.empty())
      {
        ss_requests << "No tasks were assigned to the robot.";
      }
      else
      {
        ss_requests << "The following tasks will be canceled:";
        for (const auto& a : assignments)
        {
          ss_requests << "\n -- " << a.request()->booking()->id();
        }
      }

      RCLCPP_ERROR(
        node->get_logger(),
        "Failed to reassign tasks for [%s]. %s\nReasons for failure:%s",
        ss_requests.str().c_str(),
        ss_errors.str().c_str());

      auto& fleet_impl = agv::FleetUpdateHandle::Implementation::get(*fleet);
      auto& unassigned = fleet_impl.unassigned_requests;
      const auto r_it = std::remove_if(
        unassigned.begin(),
        unassigned.end(),
        [&assignments](const auto& r)
        {
          return std::find_if(
            assignments.begin(),
            assignments.end(),
            [r](const auto& a)
            {
              return a.request() == r;
            }) != assignments.end();
        });
      unassigned.erase(r_it, unassigned.end());

      for (const auto& a : assignments)
      {
        self->_publish_canceled_pending_task(a, {"Failure to reassign"});
        self->_register_executed_task(a.request()->booking()->id());
      }

      on_failure(errors);
    });
}

//==============================================================================
TaskManager::RobotModeMsg TaskManager::robot_mode() const
{
  const auto mode = rmf_fleet_msgs::build<RobotModeMsg>()
    .mode(_active_task.is_finished() ?
      RobotModeMsg::MODE_IDLE :
      _context->current_mode())
    .mode_request_id(0)
    .performing_action("");

  return mode;
}

//==============================================================================
std::vector<nlohmann::json> TaskManager::task_log_updates() const
{
  std::vector<nlohmann::json> logs;
  for (const auto& it : _task_logs)
  {
    nlohmann::json update_msg = _task_log_update_msg;
    update_msg["data"] = it.second;
    std::string error = "";
    if (_validate_json(
        update_msg, rmf_api_msgs::schemas::task_log_update, error))
    {
      logs.push_back(update_msg);
    }
    else
    {
      RCLCPP_ERROR(
        _context->node()->get_logger(),
        "%s", error.c_str());
    }
  }
  return logs;
}

//==============================================================================
nlohmann::json TaskManager::submit_direct_request(
  const nlohmann::json& request,
  const std::string& request_id)
{
  auto fleet_handle = _fleet_handle.lock();
  if (!fleet_handle)
  {
    return _make_error_response(
      18, "Shutdown", "The fleet adapter is shutting down");
  }

  const auto& fleet = _context->group();
  const auto& robot = _context->name();

  if (!_context->commission().is_accepting_direct_tasks())
  {
    return _make_error_response(
      20, "Uncommissioned", "The robot [" + robot + "] in fleet ["
      + fleet + "] is not commissioned to perform direct tasks.");
  }

  const auto& impl =
    agv::FleetUpdateHandle::Implementation::get(*fleet_handle);
  std::vector<std::string> errors;
  const auto new_request = impl.convert(request_id, request, errors);
  if (!new_request)
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Unable to generate a valid request for direct task [%s]:\n%s",
      request_id.c_str(),
      request.dump().c_str());

    nlohmann::json response_json;
    response_json["success"] = false;
    std::vector<nlohmann::json> json_errors = {};
    for (const auto& e : errors)
    {
      RCLCPP_ERROR(_context->node()->get_logger(), "%s", e.c_str());
      try
      {
        auto error = nlohmann::json::parse(e);
        json_errors.push_back(error);
      }
      catch (const std::exception&)
      {
        nlohmann::json error;
        error["code"] = 42;
        error["category"] = "unknown";
        error["detail"] = e;
        json_errors.push_back(error);
      }
    }
    response_json["errors"] = std::move(json_errors);

    return response_json;
  }
  // Generate Assignment for the request
  const auto task_planner = _context->task_planner();
  if (!task_planner)
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Fleet [%s] is not configured with parameters for task planning."
      "Use FleetUpdateHandle::set_task_planner_params(~) to set the "
      "parameters required.", fleet.c_str());

    return _make_error_response(
      19, "Misconfigured",
      "The fleet adapter is not configured for task planning");
  }

  const auto current_state = expected_finish_state();
  const auto& constraints = task_planner->configuration().constraints();
  const auto& parameters = task_planner->configuration().parameters();
  const auto model = new_request->description()->make_model(
    new_request->booking()->earliest_start_time(),
    parameters);
  const auto estimate = model->estimate_finish(
    current_state,
    constraints,
    *_travel_estimator);

  rmf_task::State finish_state;
  rmf_traffic::Time deployment_time;

  if (!estimate.has_value())
  {
    RCLCPP_WARN(
      _context->node()->get_logger(),
      "Unable to estimate final state for direct task request [%s]. This may "
      "be due to insufficient resources to perform the task. The task will be "
      "still be added to the queue.",
      request_id.c_str());
    finish_state = current_state;
    deployment_time = new_request->booking()->earliest_start_time();
  }
  else
  {
    finish_state = estimate.value().finish_state();
    deployment_time = estimate.value().wait_until();
  }

  const DirectAssignment assignment = DirectAssignment{
    _next_sequence_number,
    Assignment(
      new_request,
      finish_state,
      deployment_time)
  };
  ++_next_sequence_number;
  {
    std::lock_guard<std::recursive_mutex> lock(_mutex);
    _direct_queue.insert(assignment);
  }

  RCLCPP_INFO(
    _context->node()->get_logger(),
    "Direct request [%s] successfully queued for robot [%s]",
    request_id.c_str(),
    robot.c_str());

  // Publish api response
  nlohmann::json response_json;
  response_json["success"] = true;
  nlohmann::json task_state;
  copy_booking_data(task_state["booking"], *new_request->booking());
  // task_state["category"] = request["category"].get<std::string>();
  task_state["detail"] = request["description"];
  task_state["status"] = "queued";
  auto& dispatch = task_state["dispatch"];
  dispatch["status"] = "queued";
  auto& assign = task_state["assigned_to"];
  assign["group"] = fleet;
  assign["name"] = robot;
  response_json["state"] = task_state;

  return response_json;
}

//==============================================================================
void TaskManager::Interruption::resume(std::vector<std::string> labels)
{
  std::lock_guard<std::mutex> lock(mutex);
  if (resumed)
    return;

  resumed = true;
  if (const auto mgr = w_mgr.lock())
  {
    mgr->_context->worker().schedule(
      [w = mgr->weak_from_this(),
      token_map = std::move(token_map),
      labels = std::move(labels)](const auto&)
      {
        const auto mgr = w.lock();
        if (!mgr)
          return;

        const auto now = mgr->_context->now();
        for (auto* task : {
          &mgr->_active_task,
          &mgr->_emergency_pullover,
          &mgr->_waiting
        })
        {
          if (*task)
          {
            const auto token_it = token_map.find(task->id());
            if (token_it == token_map.end())
              continue;

            task->remove_interruption(
              {token_it->second}, std::move(labels), now);
          }
        }
      });
  }
}

//==============================================================================
TaskManager::Interruption::~Interruption()
{
  resume({"automatic release"});
}

//==============================================================================
void TaskManager::interrupt_robot(
  std::shared_ptr<Interruption> interruption,
  std::vector<std::string> labels,
  std::function<void()> robot_is_interrupted)
{
  _robot_interrupts.push_back(
    RobotInterrupt{
      std::move(interruption),
      std::move(labels),
      std::move(robot_is_interrupted)
    });

  _process_robot_interrupts();
}

namespace {
//==============================================================================
std::vector<std::string> get_labels(const nlohmann::json& request)
{
  const auto labels_it = request.find("labels");
  if (labels_it != request.end())
    return labels_it->get<std::vector<std::string>>();

  return {};
}
} // anonymous namespace

//==============================================================================
bool TaskManager::cancel_task(
  const std::string& task_id,
  std::vector<std::string> labels)
{
  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    _active_task.cancel(std::move(labels), _context->now());
    return true;
  }

  // TODO(YV): We could cache the task_ids of direct and dispatched tasks in
  // unordered_sets and perform a lookup to see which function to call.
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  if (_cancel_task_from_dispatch_queue(task_id, labels))
    return true;

  if (_cancel_task_from_direct_queue(task_id, labels))
    return true;

  return false;
}

//==============================================================================
bool TaskManager::kill_task(
  const std::string& task_id,
  std::vector<std::string> labels)
{
  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    _active_task.kill(std::move(labels), _context->now());
    return true;
  }

  std::lock_guard<std::recursive_mutex> lock(_mutex);
  if (_cancel_task_from_dispatch_queue(task_id, labels))
    return true;

  if (_cancel_task_from_direct_queue(task_id, labels))
    return true;

  return false;
}

//==============================================================================
bool TaskManager::quiet_cancel_task(
  const std::string& task_id,
  std::vector<std::string> labels)
{
  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    _active_task.quiet_cancel(std::move(labels), _context->now());
    return true;
  }

  return false;
}

//==============================================================================
void TaskManager::_cancel_idle_behavior(std::vector<std::string> labels)
{
  if (_waiting)
  {
    _waiting.cancel(std::move(labels), _context->now());
  }
}

//==============================================================================
void TaskManager::_begin_next_task()
{
  if (_active_task)
  {
    return;
  }

  if (_emergency_active)
  {
    return;
  }

  std::lock_guard<std::recursive_mutex> guard(_mutex);

  if (_queue.empty() && _direct_queue.empty())
  {
    if (!_waiting && !_finished_waiting)
    {
      _begin_waiting();
    }

    return;
  }

  // The next task should one in the direct assignment queue if present
  const bool is_next_task_direct = !_direct_queue.empty();
  const auto assignment = is_next_task_direct ?
    _direct_queue.begin()->assignment :
    _queue.front();

  // We take the minimum of the two to deal with cases where the deployment_time
  // as computed by the task planner is greater than the earliest_start_time
  // which is greater than now. This can happen for example if the previous task
  // completed earlier than estimated.
  // TODO: Reactively replan task assignments across agents in a fleet every
  // time as task is completed.
  const auto deployment_time = std::min(
    assignment.request()->booking()->earliest_start_time(),
    assignment.deployment_time());

  const rmf_traffic::Time now = rmf_traffic_ros2::convert(
    _context->node()->now());

  if (now >= deployment_time)
  {
    if (_waiting)
    {
      _waiting.cancel({"New task ready"}, _context->now());
      return;
    }

    // Update state in RobotContext and Assign active task
    const auto& id = assignment.request()->booking()->id();
    RCLCPP_INFO(
      _context->node()->get_logger(),
      "Beginning next task [%s] for robot [%s]",
      id.c_str(),
      _context->requester_id().c_str());

    _finished_waiting = false;
    _context->current_task_end_state(assignment.finish_state());
    _context->current_task_id(id);
    _active_task = ActiveTask::start(
      _context->task_activator()->activate(
        _context->make_get_state(),
        _context->task_parameters(),
        *assignment.request(),
        _update_cb(),
        _checkpoint_cb(),
        _phase_finished_cb(),
        _task_finished(id)),
      _context->now());

    if (is_next_task_direct)
      _direct_queue.erase(_direct_queue.begin());
    else
      _queue.erase(_queue.begin());

    if (!_active_task)
    {
      const auto info = assignment.request()->description()->generate_info(
        _context->make_get_state()(), *_context->task_parameters());
      RCLCPP_ERROR(
        _context->node()->get_logger(),
        "[rmf_fleet_adapter::TaskManager::_begin_next_task] Failed to "
        "instantiate task type [%s] for ID [%s]. This indicates that the "
        "fleet adapter is incorrectly configured.",
        info.category.c_str(),
        assignment.request()->booking()->id().c_str());

      _context->worker().schedule(
        [w = weak_from_this()](const auto&)
        {
          if (const auto self = w.lock())
            self->_begin_next_task();
        });

      return;
    }

    RCLCPP_INFO(
      _context->node()->get_logger(),
      "Beginning new task [%s] for [%s]. Remaining queue size: %ld",
      _active_task.id().c_str(),
      _context->requester_id().c_str(),
      _queue.size());

    _register_executed_task(_active_task.id());
  }
  else
  {
    if (!_waiting && !_finished_waiting)
    {
      _begin_waiting();
    }
  }

  _context->worker().schedule(
    [w = weak_from_this()](const auto&)
    {
      if (const auto self = w.lock())
        self->_process_robot_interrupts();
    });
}

//==============================================================================
void TaskManager::_begin_pullover()
{
  _finished_waiting = false;
  auto task_id = "emergency_pullover." + _context->name() + "."
    + _context->group() + "-"
    + std::to_string(_count_emergency_pullover++);
  _context->current_task_id(task_id);

  // TODO(MXG): Consider subscribing to the emergency pullover update
  _emergency_pullover = ActiveTask::start(
    events::EmergencyPullover::start(
      task_id,
      _context,
      _update_cb(),
      _make_resume_from_emergency()),
    _context->now());

  _context->worker().schedule(
    [w = weak_from_this()](const auto&)
    {
      if (const auto self = w.lock())
        self->_process_robot_interrupts();
    });

}

//==============================================================================
void TaskManager::_process_robot_interrupts()
{
  const auto now = _context->now();
  for (auto& r : _robot_interrupts)
  {
    const auto interruption = r.interruption.lock();
    if (!interruption)
      continue;

    std::lock_guard<std::mutex> lock(interruption->mutex);
    if (interruption->resumed)
      continue;

    for (auto* task : {&_active_task, &_emergency_pullover, &_waiting})
    {
      if (!*task)
        continue;

      const auto [it, inserted] =
        interruption->token_map.insert({task->id(), ""});

      if (inserted)
      {
        it->second = task->add_interruption(
          r.labels, now, _robot_interruption_callback());
      }
    }
  }

  // Clear out expired interruptions so they don't leak memory.
  const auto remove_it = std::remove_if(
    _robot_interrupts.begin(),
    _robot_interrupts.end(),
    [](const auto& r) { return !r.interruption.lock(); });

  _robot_interrupts.erase(remove_it, _robot_interrupts.end());
}

//==============================================================================
std::function<void()> TaskManager::_robot_interruption_callback()
{
  return [w = weak_from_this()]()
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_context->worker().schedule(
        [w = self->weak_from_this()](const auto&)
        {
          const auto self = w.lock();
          if (!self)
            return;

          for (auto* task : {
            &self->_active_task,
            &self->_emergency_pullover,
            &self->_waiting
          })
          {
            if ((*task) && !task->is_interrupted())
            {
              return;
            }
          }

          // Both the active task and the emergency pullover are either idle or
          // interrupted, so now we can trigger the interruption ready callback
          // on any robot interruptions that are waiting for it.
          for (auto& r : self->_robot_interrupts)
          {
            if (r.robot_is_interrupted)
            {
              r.robot_is_interrupted();
              r.robot_is_interrupted = nullptr;
            }
          }
        });
    };
}

//==============================================================================
void TaskManager::_begin_waiting()
{
  if (!_context->commission().is_performing_idle_behavior())
  {
    // This robot is not supposed to perform its idle behavior, so we
    // immediately from here.
    return;
  }

  if (_idle_task)
  {
    const auto request = _idle_task->make_request(_context->make_get_state()());
    _waiting = ActiveTask::start(
      _context->task_activator()->activate(
        _context->make_get_state(),
        _context->task_parameters(),
        *request,
        _update_cb(),
        _checkpoint_cb(),
        _phase_finished_cb(),
        _make_resume_from_waiting()),
      _context->now());
    _context->current_task_id(request->booking()->id());
    return;
  }

  if (!_responsive_wait_enabled)
    return;

  if (_context->location().empty())
  {
    RCLCPP_WARN(
      _context->node()->get_logger(),
      "Unable to perform responsive wait for [%s] because its position on its "
      "navigation graph is unknown. This may require operator intervention.",
      _context->requester_id().c_str());
    return;
  }

  // Determine the waypoint closest to the robot
  std::size_t waiting_point = _context->location().front().waypoint();
  double min_dist = std::numeric_limits<double>::max();
  const auto& robot_position = _context->position();
  for (const auto& start : _context->location())
  {
    const auto waypoint = start.waypoint();
    const auto& waypoint_location =
      _context->navigation_graph().get_waypoint(waypoint).get_location();
    const auto dist = (robot_position.block<2, 1>(0, 0) -
      waypoint_location).norm();
    if (dist < min_dist)
    {
      min_dist = dist;
      waiting_point = waypoint;
    }
  }

  const auto task_id = "wait." + _context->name() + "."
    + _context->group() + "-"
    + std::to_string(_count_waiting++);

  _waiting = ActiveTask::start(
    events::ResponsiveWait::start(
      task_id,
      _context,
      waiting_point,
      _update_cb(),
      _make_resume_from_waiting()),
    _context->now());
  _context->current_task_id(task_id);
}

//==============================================================================
std::function<void()> TaskManager::_make_resume_from_emergency()
{
  return [w = weak_from_this()]()
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_resume_from_emergency();
    };
}

//==============================================================================
void TaskManager::_resume_from_emergency()
{
  _context->worker().schedule(
    [w = weak_from_this()](const auto&)
    {
      const auto self = w.lock();
      if (!self)
        return;

      if (self->_emergency_active)
      {
        return;
      }

      self->_emergency_pullover = ActiveTask();

      if (!self->_emergency_pullover_interrupt_token.has_value())
      {
        self->_begin_next_task();
        return;
      }

      if (self->_active_task)
      {
        self->_active_task.remove_interruption(
          {*self->_emergency_pullover_interrupt_token},
          {"emergency finished"},
          self->_context->now());
        self->_emergency_pullover_interrupt_token = std::nullopt;
      }
      else
      {
        self->_begin_next_task();
      }
    });
}

//==============================================================================
std::function<void()> TaskManager::_make_resume_from_waiting()
{
  return [w = weak_from_this()]()
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_context->worker().schedule(
        [w = self->weak_from_this()](const auto&)
        {
          const auto self = w.lock();
          if (!self)
            return;

          // This condition deals with an awkward edge case where idle behavior
          // would not restart when toggling the idle behavior commission back
          // on. We fix this by keeping the _finished_waiting flag clean if
          // idle behavior commissioning is off, so there's nothing to block
          // idle behavior from beginning again if it gets toggled back on.
          if (self->_context->commission().is_performing_idle_behavior())
          {
            self->_finished_waiting = true;
          }

          self->_waiting = ActiveTask();
          self->_begin_next_task();

          if (self->_emergency_active)
          {
            self->_begin_pullover();
          }
        });
    };
}

//==============================================================================
void TaskManager::configure_retreat_to_charger(
  std::optional<rmf_traffic::Duration> duration)
{
  if (duration.has_value() && *duration <= rmf_traffic::Duration(0))
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "[TaskManager::configure_retreat_to_charger] "
      "Invalid value for duration: %f",
      rmf_traffic::time::to_seconds(*duration));
  }

  if (!duration.has_value() || *duration <= rmf_traffic::Duration(0))
  {
    if (_retreat_timer && !_retreat_timer->is_canceled())
    {
      _retreat_timer->cancel();
    }
    return;
  }

  if (_retreat_timer)
    _retreat_timer->reset();
  _retreat_timer = _context->node()->try_create_wall_timer(
    duration.value(),
    [w = weak_from_this()]()
    {
      if (auto mgr = w.lock())
      {
        mgr->retreat_to_charger();
      }
    });
}

//==============================================================================
void TaskManager::retreat_to_charger()
{
  if (!_travel_estimator)
    return;

  {
    std::lock_guard<std::recursive_mutex> guard(_mutex);
    if (_active_task || !_queue.empty())
      return;
  }

  const auto task_planner = _context->task_planner();
  if (!task_planner)
    return;

  if (!task_planner->configuration().constraints().drain_battery())
    return;

  const auto current_state = expected_finish_state();
  const auto charging_waypoint =
    current_state.dedicated_charging_waypoint().value();
  if (current_state.waypoint() == charging_waypoint)
    return;

  const auto& constraints = task_planner->configuration().constraints();
  const double threshold_soc = constraints.threshold_soc();
  const double retreat_threshold = 1.2 * threshold_soc; // safety factor
  const double current_battery_soc = _context->current_battery_soc();

  const auto& parameters = task_planner->configuration().parameters();
  // TODO(YV): Expose the TravelEstimator in the TaskPlanner to benefit from
  // caching
  const rmf_traffic::agv::Planner::Goal retreat_goal{charging_waypoint};
  const auto result = _travel_estimator->estimate(
    current_state.extract_plan_start().value(), retreat_goal);
  if (!result.has_value())
  {
    RCLCPP_WARN(
      _context->node()->get_logger(),
      "Unable to compute estimate of journey back to charger for robot [%s]",
      _context->name().c_str());
    return;
  }

  const double battery_soc_after_retreat =
    current_battery_soc - result->change_in_charge();

  if ((battery_soc_after_retreat < retreat_threshold) &&
    (battery_soc_after_retreat > threshold_soc))
  {
    // Add a new charging task to the task queue
    const auto charging_request = rmf_task::requests::ChargeBattery::make(
      current_state.time().value(),
      _context->requester_id(),
      rmf_traffic_ros2::convert(_context->node()->now()),
      nullptr,
      true);
    const auto model = charging_request->description()->make_model(
      current_state.time().value(),
      parameters);

    const auto finish = model->estimate_finish(
      current_state,
      constraints,
      *_travel_estimator);

    if (!finish)
      return;

    rmf_task::TaskPlanner::Assignment charging_assignment(
      charging_request,
      finish.value().finish_state(),
      current_state.time().value());

    const DirectAssignment assignment = DirectAssignment{
      _next_sequence_number,
      charging_assignment};
    ++_next_sequence_number;
    {
      std::lock_guard<std::recursive_mutex> lock(_mutex);
      _direct_queue.insert(assignment);
    }

    RCLCPP_INFO(
      _context->node()->get_logger(),
      "Initiating automatic retreat to charger for robot [%s]",
      _context->name().c_str());
  }

  if ((battery_soc_after_retreat < retreat_threshold) &&
    (battery_soc_after_retreat < threshold_soc))
  {
    RCLCPP_WARN(
      _context->node()->get_logger(),
      "Robot [%s] needs to be charged but has insufficient battery remaining "
      "to retreat to its designated charger.",
      _context->name().c_str());
  }
}

//==============================================================================
const std::vector<std::string>& TaskManager::get_executed_tasks() const
{
  return _executed_task_registry;
}

//==============================================================================
void TaskManager::_register_executed_task(const std::string& id)
{
  // Currently the choice of storing 100 executed tasks is arbitrary.
  // TODO: Save a time stamp for when tasks are completed and cull entries after
  // a certain time window instead.
  if (_executed_task_registry.size() >= 100)
    _executed_task_registry.erase(_executed_task_registry.begin());

  _executed_task_registry.push_back(id);
}

//==============================================================================
void TaskManager::_populate_task_summary(
  std::shared_ptr<LegacyTask> task,
  uint32_t task_summary_state,
  TaskManager::TaskSummaryMsg& msg)
{
  if (task == nullptr) // ResponsiveWait
  {
    msg.task_id = _context->requester_id() + ":waiting";

    msg.start_time = _context->node()->now();
    msg.end_time = _queue.empty() ? msg.start_time : rmf_traffic_ros2::convert(
      _queue.front().deployment_time());
    // Make the task type explicit
    msg.task_profile.description.task_type.type =
      rmf_task_msgs::msg::TaskType::TYPE_STATION;
  }

  else
  {
    msg.task_id = task->id();
    msg.start_time = rmf_traffic_ros2::convert(
      task->deployment_time());
    msg.end_time = rmf_traffic_ros2::convert(
      task->finish_state().time().value());
    msg.task_profile = task->task_profile();
  }

  msg.fleet_name = _context->description().owner();
  msg.robot_name = _context->name();

  msg.state = task_summary_state;
}

//==============================================================================
void TaskManager::_schema_loader(
  const nlohmann::json_uri& id, nlohmann::json& value) const
{
  const auto it = _schema_dictionary.find(id.url());
  if (it == _schema_dictionary.end())
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "[TaskManager] url: %s not found in schema dictionary", id.url().c_str());
    return;
  }

  value = it->second;
}

//==============================================================================
void TaskManager::_validate_and_publish_websocket(
  const nlohmann::json& msg,
  const nlohmann::json_schema::json_validator& validator) const
{
  std::string error = "";
  if (!_validate_json(msg, validator, error))
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Failed to validate message [%s]: [%s]",
      msg.dump().c_str(),
      error.c_str());
    return;
  }

  if (!_broadcast_client.has_value())
    return;

  const auto client = _broadcast_client->lock();
  if (!client)
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Unable to lock BroadcastClient within TaskManager of robot [%s]",
      _context->name().c_str());
    return;
  }
  client->publish(msg);
}

//==============================================================================
void TaskManager::_validate_and_publish_api_response(
  const nlohmann::json& response,
  const nlohmann::json_schema::json_validator& validator,
  const std::string& request_id)
{
  std::string error;
  if (!_validate_json(response, validator, error))
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Error in response to [%s]: %s",
      request_id.c_str(),
      error.c_str());
    return;
  }

  _context->node()->task_api_response()->publish(
    rmf_task_msgs::build<rmf_task_msgs::msg::ApiResponse>()
    .type(rmf_task_msgs::msg::ApiResponse::TYPE_RESPONDING)
    .json_msg(response.dump())
    .request_id(request_id));
}

//==============================================================================
rmf_task::State TaskManager::_get_state() const
{
  return _context->current_task_end_state();
}

//==============================================================================
void TaskManager::_consider_publishing_updates()
{
  const auto now = std::chrono::steady_clock::now();
  const auto time_elapsed = now - _last_update_time;
  // TODO(MXG): Make max elapsed time configurable
  const auto max_time_elapsed = std::chrono::seconds(1);
  if (_task_state_update_available || time_elapsed > max_time_elapsed)
  {
    _task_state_update_available = false;
    _last_update_time = now;
    _publish_task_state();
  }
}

//==============================================================================
void TaskManager::_publish_task_state()
{
  if (!_active_task)
    return;

  _active_task.publish_task_state(*this);
}

//==============================================================================
rmf_task::State TaskManager::_publish_pending_task(
  const Assignment& pending,
  rmf_task::State expected_state,
  const rmf_task::Parameters& parameters)
{
  const auto info = pending.request()->description()->generate_info(
    std::move(expected_state), parameters);
  PendingInfo cache;
  cache.info = info;

  nlohmann::json pending_json;
  const auto& booking = *pending.request()->booking();
  copy_booking_data(pending_json["booking"], booking);

  pending_json["category"] = info.category;
  pending_json["detail"] = info.detail;

  pending_json["unix_millis_start_time"] =
    to_millis(pending.deployment_time().time_since_epoch()).count();

  if (pending.finish_state().time())
  {
    PendingTimeInfo t;
    t.unix_millis_finish_time =
      to_millis(pending.finish_state().time()->time_since_epoch()).count();

    const auto estimate =
      pending.finish_state().time().value() - pending.deployment_time();
    t.original_estimate_millis = std::max(0l, to_millis(estimate).count());

    pending_json["unix_millis_finish_time"] = t.unix_millis_finish_time;
    pending_json["original_estimate_millis"] = t.original_estimate_millis;

    cache.time = t;
  }
  copy_assignment(pending_json["assigned_to"], *_context);
  pending_json["status"] = "queued";

  auto task_state_update = _task_state_update_json;
  task_state_update["data"] = pending_json;

  static const auto validator =
    _make_validator(rmf_api_msgs::schemas::task_state_update);

  _validate_and_publish_websocket(task_state_update, validator);

  _pending_task_info[pending.request()] = cache;
  return pending.finish_state();
}

//==============================================================================
void TaskManager::_publish_task_queue()
{
  rmf_task::State expected_state = _context->current_task_end_state();
  const auto& parameters = *_context->task_parameters();
  _pending_task_info.clear();

  for (const auto& pending : _direct_queue)
  {
    expected_state = _publish_pending_task(
      pending.assignment, std::move(expected_state), parameters);
  }

  for (const auto& pending : _queue)
  {
    expected_state = _publish_pending_task(
      pending, std::move(expected_state), parameters);
  }
}

//==============================================================================
void TaskManager::_publish_canceled_pending_task(
  const Assignment& pending,
  std::vector<std::string> labels)
{
  nlohmann::json pending_json;
  const auto& booking = *pending.request()->booking();
  copy_booking_data(pending_json["booking"], booking);

  const auto info_it = _pending_task_info.find(pending.request());
  if (info_it != _pending_task_info.end())
  {
    const auto& cache = info_it->second;
    pending_json["category"] = cache.info.category;
    pending_json["detail"] = cache.info.detail;
    if (cache.time.has_value())
    {
      const auto& t = *cache.time;
      pending_json["unix_millis_finish_time"] = t.unix_millis_finish_time;
      pending_json["original_estimate_millis"] = t.original_estimate_millis;
    }
  }

  pending_json["unix_millis_start_time"] =
    to_millis(pending.deployment_time().time_since_epoch()).count();

  copy_assignment(pending_json["assigned_to"], *_context);
  pending_json["status"] = "canceled";

  nlohmann::json cancellation;
  cancellation["unix_millis_request_time"] =
    to_millis(_context->now().time_since_epoch()).count();
  cancellation["labels"] = std::move(labels);
  pending_json["cancellation"] = std::move(cancellation);

  auto task_state_update = _task_state_update_json;
  task_state_update["data"] = pending_json;

  static const auto validator =
    _make_validator(rmf_api_msgs::schemas::task_state_update);

  _validate_and_publish_websocket(task_state_update, validator);
}

//==============================================================================
auto TaskManager::_drain_dispatched_assignments() -> std::vector<Assignment>
{
  std::vector<Assignment> assignments;
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  for (const auto& a : _queue)
  {
    if (a.request()->booking()->automatic())
    {
      continue;
    }

    assignments.push_back(a);
  }
  _queue.clear();

  return assignments;
}

//==============================================================================
auto TaskManager::_drain_direct_assignments() -> std::vector<Assignment>
{
  std::vector<Assignment> assignments;
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  for (const auto& a : _direct_queue)
  {
    if (a.assignment.request()->booking()->automatic())
    {
      continue;
    }

    assignments.push_back(a.assignment);
  }
  _direct_queue.clear();

  return assignments;
}

//==============================================================================
bool TaskManager::_cancel_task_from_dispatch_queue(
  const std::string& task_id,
  const std::vector<std::string>& labels)
{
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  for (auto it = _queue.begin(); it != _queue.end(); ++it)
  {
    if (it->request()->booking()->id() == task_id)
    {
      _publish_canceled_pending_task(*it, labels);
      _queue.erase(it);

      // Count this as an executed task so we don't lose track of its existence
      _register_executed_task(task_id);
      return true;
    }
  }
  return false;
}

//==============================================================================
bool TaskManager::_cancel_task_from_direct_queue(
  const std::string& task_id,
  const std::vector<std::string>& labels)
{
  for (auto it = _direct_queue.begin(); it != _direct_queue.end(); ++it)
  {
    if (it->assignment.request()->booking()->id() == task_id)
    {
      _publish_canceled_pending_task(it->assignment, labels);
      _direct_queue.erase(it);
      return true;
    }
  }
  return false;
}

//==============================================================================
bool TaskManager::_validate_json(
  const nlohmann::json& json,
  const nlohmann::json_schema::json_validator& validator,
  std::string& error) const
{
  try
  {
    validator.validate(json);
  }
  catch (const std::exception& e)
  {
    error = e.what();
    return false;
  }

  return true;
}

//==============================================================================
bool TaskManager::_validate_request_message(
  const nlohmann::json& request_json,
  const nlohmann::json_schema::json_validator& request_validator,
  const std::string& request_id)
{
  std::string error;
  if (_validate_json(request_json, request_validator, error))
    return true;

  _send_simple_error_response(
    request_id, 5, "Invalid request format", std::move(error));
  return false;
}

//==============================================================================
void TaskManager::_send_simple_success_response(const std::string& request_id)
{
  static const auto response = make_simple_success_response();

  static const auto simple_response_validator =
    _make_validator(rmf_api_msgs::schemas::simple_response);

  _validate_and_publish_api_response(
    response, simple_response_validator, request_id);
}

//==============================================================================
void TaskManager::_send_token_success_response(
  std::string token,
  const std::string& request_id)
{
  nlohmann::json response;
  response["success"] = true;
  response["token"] = std::move(token);

  static const auto token_response_validator =
    _make_validator(rmf_api_msgs::schemas::token_response);

  _validate_and_publish_api_response(
    response, token_response_validator, request_id);
}

//==============================================================================
nlohmann::json_schema::json_validator TaskManager::_make_validator(
  const nlohmann::json& schema) const
{
  return nlohmann::json_schema::json_validator(
    schema,
    [w = weak_from_this()](const nlohmann::json_uri& id,
    nlohmann::json& value)
    {
      const auto self = w.lock();
      if (!self)
        return;
      self->_schema_loader(id, value);
    });
}

//==============================================================================
void TaskManager::_send_simple_error_response(
  const std::string& request_id,
  uint64_t code,
  std::string category,
  std::string detail)
{
  static const auto error_validator =
    _make_validator(rmf_api_msgs::schemas::simple_response);

  _validate_and_publish_api_response(
    _make_error_response(code, std::move(category), std::move(detail)),
    error_validator,
    request_id);
}

//==============================================================================
void TaskManager::_send_simple_error_if_queued(
  const std::string& task_id,
  const std::string& request_id,
  const std::string& type)
{
  // TODO(YV): We could cache the task_ids of direct and dispatched tasks in
  // unordered_sets and perform a lookup to see which queue to iterate.
  std::lock_guard<std::recursive_mutex> lock(_mutex);
  for (const auto& a : _queue)
  {
    if (a.request()->booking()->id() == task_id)
    {
      return _send_simple_error_response(
        request_id, 6, "Invalid Circumstances",
        type + " a task that is queued (not yet active) "
        "is not currently supported");
    }
  }

  for (const auto& a : _direct_queue)
  {
    if (a.assignment.request()->booking()->id() == task_id)
    {
      return _send_simple_error_response(
        request_id, 6, "Invalid Circumstances",
        type + " a task that is queued (not yet active) "
        "is not currently supported");
    }
  }
}

//==============================================================================
nlohmann::json TaskManager::_make_error_response(
  uint64_t code,
  std::string category,
  std::string detail)
{
  nlohmann::json response;
  response["success"] = false;

  nlohmann::json error;
  error["code"] = code;
  error["category"] = std::move(category);
  error["detail"] = std::move(detail);

  response["errors"] = std::vector<nlohmann::json>({std::move(error)});

  return response;
}

//==============================================================================
std::function<void(rmf_task::Phase::ConstSnapshotPtr)>
TaskManager::_update_cb()
{
  return [w = weak_from_this()](rmf_task::Phase::ConstSnapshotPtr)
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_task_state_update_available = true;
      // TODO(MXG): Use this callback to make the state updates more efficient
    };
}

//==============================================================================
std::function<void(rmf_task::Task::Active::Backup)>
TaskManager::_checkpoint_cb()
{
  return [w = weak_from_this()](rmf_task::Task::Active::Backup)
    {
      const auto self = w.lock();
      if (!self)
        return;

      // TODO(MXG): Save the backup
    };
}

//==============================================================================
std::function<void(rmf_task::Phase::ConstCompletedPtr)>
TaskManager::_phase_finished_cb()
{
  return [w = weak_from_this()](rmf_task::Phase::ConstCompletedPtr)
    {
      const auto self = w.lock();
      if (!self)
        return;

      self->_task_state_update_available = true;
      // TODO(MXG): Use this callback to make the state updates more efficient
    };
}

//==============================================================================
std::function<void()> TaskManager::_task_finished(std::string id)
{
  return [w = weak_from_this(), id]()
    {
      const auto self = w.lock();
      if (!self)
        return;

      // Publish the final state of the task before destructing it
      self->_publish_task_state();
      self->_active_task = ActiveTask();
      self->_context->current_task_id(std::nullopt);

      self->_context->worker().schedule(
        [w = self->weak_from_this()](const auto&)
        {
          if (const auto self = w.lock())
            self->_begin_next_task();
        });
    };
}

//==============================================================================
void TaskManager::_handle_request(
  const std::string& request_msg,
  const std::string& request_id)
{
  nlohmann::json request_json;
  try
  {
    request_json = nlohmann::json::parse(request_msg);
  }
  catch (const std::exception& e)
  {
    RCLCPP_ERROR(
      _context->node()->get_logger(),
      "Error parsing json_msg: %s",
      e.what());
    return;
  }

  const auto type_it = request_json.find("type");
  if (type_it == request_json.end())
    return;

  const auto& type = type_it.value();
  if (!type.is_string())
    return;

  try
  {
    const auto& type_str = type.get<std::string>();
    if (type_str == "cancel_task_request")
      _handle_cancel_request(request_json, request_id);
    else if (type_str == "kill_task_request")
      _handle_kill_request(request_json, request_id);
    else if (type_str == "interrupt_task_request")
      _handle_interrupt_request(request_json, request_id);
    else if (type_str == "resume_task_request")
      _handle_resume_request(request_json, request_id);
    else if (type_str == "rewind_task_request")
      _handle_rewind_request(request_json, request_id);
    else if (type_str == "skip_phase_request")
      _handle_skip_phase_request(request_json, request_id);
    else if (type_str == "undo_phase_skip_request")
      _handle_undo_skip_phase_request(request_json, request_id);
    else if (type_str == "robot_task_request")
      _handle_direct_request(request_json, request_id);
    else if (type_str == "robot_commission_request")
      _handle_commission_request(request_json, request_id);
    else
      return;
  }
  catch (const std::exception& e)
  {
    RCLCPP_WARN(
      _context->node()->get_logger(),
      "Encountered exception while handling a request: %s", e.what());
  }
}

//==============================================================================
void TaskManager::_handle_direct_request(
  const  nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::robot_task_request);

  static const auto response_validator =
    _make_validator(rmf_api_msgs::schemas::robot_task_response);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& fleet = request_json["fleet"].get<std::string>();
  if (fleet != _context->group())
    return;

  const auto& robot = request_json["robot"].get<std::string>();
  if (robot != _context->name())
    return;

  const nlohmann::json& request = request_json["request"];
  const auto response = submit_direct_request(request, request_id);
  _validate_and_publish_api_response(response, response_validator, request_id);
}

//==============================================================================
namespace {
nlohmann::json simple_success_json()
{
  nlohmann::json successful_result;
  successful_result["success"] = true;
  return successful_result;
}
} // anonymous namespace

//==============================================================================
void TaskManager::_handle_commission_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::robot_commission_request);

  static const auto response_validator =
    std::make_shared<nlohmann::json_schema::json_validator>(
    _make_validator(rmf_api_msgs::schemas::robot_commission_response));

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& fleet = request_json["fleet"].get<std::string>();
  if (fleet != _context->group())
    return;

  const auto& robot = request_json["robot"].get<std::string>();
  if (robot != _context->name())
    return;

  agv::RobotUpdateHandle::Commission commission = _context->commission();
  const nlohmann::json& commission_json = request_json["commission"];
  const auto dispatch_it = commission_json.find("dispatch_tasks");
  if (dispatch_it != commission_json.end())
  {
    commission.accept_dispatched_tasks(dispatch_it->get<bool>());
  }

  const auto direct_it = commission_json.find("direct_tasks");
  if (direct_it != commission_json.end())
  {
    commission.accept_direct_tasks(direct_it->get<bool>());
  }

  const auto idle_it = commission_json.find("idle_behavior");
  if (idle_it != commission_json.end())
  {
    commission.perform_idle_behavior(idle_it->get<bool>());
  }

  _context->set_commission(commission);

  nlohmann::json response_json;
  response_json["commission"] = simple_success_json();

  const auto direct_policy_it =
    request_json.find("pending_direct_tasks_policy");
  if (direct_policy_it != request_json.end()
    && direct_policy_it->get<std::string>() == "cancel")
  {
    const auto assignments = _drain_direct_assignments();
    for (const auto& a : assignments)
    {
      _publish_canceled_pending_task(
        a, {"Canceled by robot commission request [" + request_id + "]"});
      _register_executed_task(a.request()->booking()->id());
    }
  }

  response_json["pending_direct_tasks_policy"] = simple_success_json();

  const auto dispatch_policy_it =
    request_json.find("pending_dispatch_tasks_policy");
  if (dispatch_policy_it == request_json.end()
    || dispatch_policy_it->get<std::string>() == "reassign")
  {
    reassign_dispatched_requests(
      [
        request_id,
        response_validator = response_validator,
        base_response_json = response_json,
        self = shared_from_this()
      ]()
      {
        nlohmann::json response_json = base_response_json;
        response_json["pending_dispatch_tasks_policy"] = simple_success_json();
        self->_validate_and_publish_api_response(
          response_json, *response_validator, request_id);
      },
      [
        request_id,
        response_validator = response_validator,
        base_response_json = response_json,
        self = shared_from_this()
      ](std::vector<std::string> errors)
      {
        nlohmann::json response_json = base_response_json;
        nlohmann::json dispatch_json;
        dispatch_json["success"] = false;
        std::vector<nlohmann::json> errors_json;
        for (const auto& e : errors)
        {
          nlohmann::json error;
          error["code"] = 21;
          error["category"] = "planner";
          error["detail"] = e;
          errors_json.push_back(error);
        }
        dispatch_json["errors"] = errors_json;
        response_json["pending_dispatch_tasks_policy"] = dispatch_json;
        self->_validate_and_publish_api_response(
          response_json, *response_validator, request_id);
      });
  }
  else
  {
    if (dispatch_policy_it->get<std::string>() == "cancel")
    {
      std::vector<Assignment> assignments = _drain_dispatched_assignments();
      for (const auto& a : assignments)
      {
        _publish_canceled_pending_task(
          a, {"Canceled by robot commission request [" + request_id + "]"});
        _register_executed_task(a.request()->booking()->id());
      }
    }

    response_json["pending_dispatch_tasks_policy"] = simple_success_json();
    _validate_and_publish_api_response(
      response_json, *response_validator, request_id);
  }
}

//==============================================================================
void TaskManager::_handle_cancel_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::cancel_task_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["task_id"].get<std::string>();
  if (cancel_task(task_id, get_labels(request_json)))
    _send_simple_success_response(request_id);
}

//==============================================================================
void TaskManager::_handle_kill_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::kill_task_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["task_id"].get<std::string>();
  if (kill_task(task_id, get_labels(request_json)))
    _send_simple_success_response(request_id);
}

//==============================================================================
void TaskManager::_handle_interrupt_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::interrupt_task_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["task_id"].get<std::string>();

  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    return _send_token_success_response(
      _active_task.add_interruption(
        get_labels(request_json), _context->now(), []() {}),
      request_id);
  }

  _send_simple_error_if_queued(task_id, request_id, "Interrupting");
}

//==============================================================================
void TaskManager::_handle_resume_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::resume_task_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["for_task"].get<std::string>();

  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    auto unknown_tokens = _active_task.remove_interruption(
      request_json["for_tokens"].get<std::vector<std::string>>(),
      get_labels(request_json),
      _context->now());

    if (unknown_tokens.empty())
      return _send_simple_success_response(request_id);

    std::string detail = "[";
    for (std::size_t i = 0; i < unknown_tokens.size(); ++i)
    {
      detail += unknown_tokens[i];
      if (i < unknown_tokens.size()-1)
        detail += ", ";
    }
    detail += "]";

    return _send_simple_error_response(
      request_id, 7, "Unknown Tokens", std::move(detail));
  }

  _send_simple_error_if_queued(task_id, request_id, "Resuming");
}

//==============================================================================
void TaskManager::_handle_rewind_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::rewind_task_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["task_id"].get<std::string>();

  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    _active_task.rewind(request_json["phase_id"].get<uint64_t>());
    return _send_simple_success_response(request_id);
  }

  _send_simple_error_if_queued(task_id, request_id, "Rewinding");
}

//==============================================================================
void TaskManager::_handle_skip_phase_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::skip_phase_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["task_id"].get<std::string>();

  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    return _send_token_success_response(
      _active_task.skip(
        request_json["phase_id"].get<uint64_t>(),
        get_labels(request_json),
        _context->now()),
      request_id);
  }

  _send_simple_error_if_queued(task_id, request_id, "Skipping a phase in ");
}

//==============================================================================
void TaskManager::_handle_undo_skip_phase_request(
  const nlohmann::json& request_json,
  const std::string& request_id)
{
  static const auto request_validator =
    _make_validator(rmf_api_msgs::schemas::undo_skip_phase_request);

  if (!_validate_request_message(request_json, request_validator, request_id))
    return;

  const auto& task_id = request_json["for_task"];

  if (_active_task && _active_task.id() == task_id)
  {
    _task_state_update_available = true;
    auto unknown_tokens = _active_task.remove_skips(
      request_json["for_tokens"].get<std::vector<std::string>>(),
      get_labels(request_json),
      _context->now());

    if (unknown_tokens.empty())
      return _send_simple_success_response(request_id);

    std::string detail = "[";
    for (std::size_t i = 0; i < unknown_tokens.size(); ++i)
    {
      detail += unknown_tokens[i];
      if (i < unknown_tokens.size()-1)
        detail += ", ";
    }
    detail += "]";

    return _send_simple_error_response(
      request_id, 7, "Unknown Tokens", std::move(detail));
  }

  _send_simple_error_if_queued(task_id, request_id, "Undoing a phase skip in ");
}

} // namespace rmf_fleet_adapter
