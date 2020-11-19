/*
 * Copyright 2016 The Cartographer Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cartographer/mapping/map_builder.h"

#include "cartographer/common/make_unique.h"
#include "cartographer/common/time.h"
#include "cartographer/io/internal/mapping_state_serialization.h"
#include "cartographer/io/proto_stream_deserializer.h"
#include "cartographer/mapping/internal/2d/local_trajectory_builder_2d.h"
#include "cartographer/mapping/internal/2d/overlapping_submaps_trimmer_2d.h"
#include "cartographer/mapping/internal/2d/pose_graph_2d.h"
#include "cartographer/mapping/internal/3d/local_trajectory_builder_3d.h"
#include "cartographer/mapping/internal/3d/pose_graph_3d.h"
#include "cartographer/mapping/internal/collated_trajectory_builder.h"
#include "cartographer/mapping/internal/global_trajectory_builder.h"
#include "cartographer/mapping/proto/internal/legacy_serialized_data.pb.h"
#include "cartographer/sensor/internal/collator.h"
#include "cartographer/sensor/internal/trajectory_collator.h"
#include "cartographer/sensor/internal/voxel_filter.h"
#include "cartographer/transform/rigid_transform.h"
#include "cartographer/transform/transform.h"

namespace cartographer {
namespace mapping {

namespace {

using mapping::proto::SerializedData;

std::vector<std::string> SelectRangeSensorIds(
    const std::set<MapBuilder::SensorId>& expected_sensor_ids) {
  std::vector<std::string> range_sensor_ids;
  for (const MapBuilder::SensorId& sensor_id : expected_sensor_ids) {
    if (sensor_id.type == MapBuilder::SensorId::SensorType::RANGE) {
      range_sensor_ids.push_back(sensor_id.id);
    }
  }
  return range_sensor_ids;
}

}  // namespace

proto::MapBuilderOptions CreateMapBuilderOptions(
    common::LuaParameterDictionary* const parameter_dictionary) {
  proto::MapBuilderOptions options;
  options.set_use_trajectory_builder_2d(
      parameter_dictionary->GetBool("use_trajectory_builder_2d"));
  options.set_use_trajectory_builder_3d(
      parameter_dictionary->GetBool("use_trajectory_builder_3d"));
  options.set_num_background_threads(
      parameter_dictionary->GetNonNegativeInt("num_background_threads"));
  *options.mutable_pose_graph_options() = CreatePoseGraphOptions(
      parameter_dictionary->GetDictionary("pose_graph").get());
  CHECK_NE(options.use_trajectory_builder_2d(),
           options.use_trajectory_builder_3d());
  return options;
}

/**
 * @brief 构造函数
 * @param options 配置项
 */
MapBuilder::MapBuilder(const proto::MapBuilderOptions& options)
    : options_(options), thread_pool_(options.num_background_threads()) {
  CHECK(options.use_trajectory_builder_2d() ^
        options.use_trajectory_builder_3d());
  /** 使用2D地图，构造2D的pose_graph_ */
  if (options.use_trajectory_builder_2d()) {
    pose_graph_ = common::make_unique<PoseGraph2D>(
        options_.pose_graph_options(),
        common::make_unique<optimization::OptimizationProblem2D>(
            options_.pose_graph_options().optimization_problem_options()),
        &thread_pool_);
  }
  /** 使用3D地图，构造3D的pose_graph_ */
  if (options.use_trajectory_builder_3d()) {
    pose_graph_ = common::make_unique<PoseGraph3D>(
        options_.pose_graph_options(),
        common::make_unique<optimization::OptimizationProblem3D>(
            options_.pose_graph_options().optimization_problem_options()),
        &thread_pool_);
  }
  if (options.collate_by_trajectory()) {
    sensor_collator_ = common::make_unique<sensor::TrajectoryCollator>();
  } else {
    sensor_collator_ = common::make_unique<sensor::Collator>();
  }
}

/**
 * @brief 添加轨迹构建器，是CartoGrapher中最重要的一个函数
 * @param expected_sensor_ids 
 * @param trajectory_options 轨迹配置项
 * @param local_slam_result_callback
 * @return 新的轨迹构建器id 
 */
int MapBuilder::AddTrajectoryBuilder(
    const std::set<SensorId>& expected_sensor_ids,
    const proto::TrajectoryBuilderOptions& trajectory_options,
    LocalSlamResultCallback local_slam_result_callback) {

  /** 分配新的轨迹id */
  const int trajectory_id = trajectory_builders_.size();
  
  /** 处理3D地图的情况 */
  if (options_.use_trajectory_builder_3d()) {
	/** 定义一个轨迹构建器 */
    std::unique_ptr<LocalTrajectoryBuilder3D> local_trajectory_builder;
	/** 实例化轨迹构建器 */
    if (trajectory_options.has_trajectory_builder_3d_options()) {
      local_trajectory_builder = common::make_unique<LocalTrajectoryBuilder3D>(
          trajectory_options.trajectory_builder_3d_options(),
          SelectRangeSensorIds(expected_sensor_ids));
    }
	/** 检查强制类型转换：PoseGraph转成PoseGraph3D */
    DCHECK(dynamic_cast<PoseGraph3D*>(pose_graph_.get()));
	/** 将轨迹构建器压入构建器数组 */
    trajectory_builders_.push_back(
        common::make_unique<CollatedTrajectoryBuilder>(
            sensor_collator_.get(), trajectory_id, expected_sensor_ids,
            CreateGlobalTrajectoryBuilder3D(
                std::move(local_trajectory_builder), trajectory_id,
                static_cast<PoseGraph3D*>(pose_graph_.get()),
                local_slam_result_callback)));
  } else { /** 处理2D地图的情况 */
    /** 定义一个轨迹构建器 */
    std::unique_ptr<LocalTrajectoryBuilder2D> local_trajectory_builder;
	/** 实例化轨迹构建器 */
    if (trajectory_options.has_trajectory_builder_2d_options()) {
      local_trajectory_builder = common::make_unique<LocalTrajectoryBuilder2D>(
          trajectory_options.trajectory_builder_2d_options(),
          SelectRangeSensorIds(expected_sensor_ids));
    }
	/** 检查强制类型转换：PoseGraph转成PoseGraph2D */
    DCHECK(dynamic_cast<PoseGraph2D*>(pose_graph_.get()));
	/** 将轨迹构建器压入构建器数组 */
    trajectory_builders_.push_back(
        common::make_unique<CollatedTrajectoryBuilder>(
            sensor_collator_.get(), trajectory_id, expected_sensor_ids,
            CreateGlobalTrajectoryBuilder2D(
                std::move(local_trajectory_builder), trajectory_id,
                static_cast<PoseGraph2D*>(pose_graph_.get()),
                local_slam_result_callback)));
	
	/** 2D地图还需要额外添加一个重叠submap微调器到PoseGraph中 */
    if (trajectory_options.has_overlapping_submaps_trimmer_2d()) {
      const auto& trimmer_options =
          trajectory_options.overlapping_submaps_trimmer_2d();
      pose_graph_->AddTrimmer(common::make_unique<OverlappingSubmapsTrimmer2D>(
          trimmer_options.fresh_submaps_count(),
          trimmer_options.min_covered_area() /
              common::Pow2(trajectory_options.trajectory_builder_2d_options()
                               .submaps_options()
                               .grid_options_2d()
                               .resolution()),
          trimmer_options.min_added_submaps_count()));
    }
  }
  /** 如果是纯定位，再添加一个纯定位微调器到PoseGraph中 */
  if (trajectory_options.pure_localization()) {
    constexpr int kSubmapsToKeep = 3;
    pose_graph_->AddTrimmer(common::make_unique<PureLocalizationTrimmer>(
        trajectory_id, kSubmapsToKeep));
  }
  /** 如果有初始的轨迹位姿，则添加初始位姿到PoseGraph中 */
  if (trajectory_options.has_initial_trajectory_pose()) {
    const auto& initial_trajectory_pose =
        trajectory_options.initial_trajectory_pose();
    pose_graph_->SetInitialTrajectoryPose(
        trajectory_id, initial_trajectory_pose.to_trajectory_id(),
        transform::ToRigid3(initial_trajectory_pose.relative_pose()),
        common::FromUniversal(initial_trajectory_pose.timestamp()));
  }
  
  /** 将传感器相关的配置转成protobuf流 */
  proto::TrajectoryBuilderOptionsWithSensorIds options_with_sensor_ids_proto;
  for (const auto& sensor_id : expected_sensor_ids) {
    *options_with_sensor_ids_proto.add_sensor_id() = ToProto(sensor_id);
  }
  /** 将轨迹构建器配置项添加到传感器配置项 */
  *options_with_sensor_ids_proto.mutable_trajectory_builder_options() =
      trajectory_options;
  /** 将传感器配置项压入轨迹配置项数组 */
  all_trajectory_builder_options_.push_back(options_with_sensor_ids_proto);
  /** 确认轨迹构建器数组和其配置项数组长度一致 */
  CHECK_EQ(trajectory_builders_.size(), all_trajectory_builder_options_.size());
  /** 返回新的轨迹id */
  return trajectory_id;
}

/**
 * @brief 从序列化的数据中构造一个轨迹
 * @param options_with_sensor_ids_proto
 * @return 新的轨迹构建器id 
 */
int MapBuilder::AddTrajectoryForDeserialization(
    const proto::TrajectoryBuilderOptionsWithSensorIds&
        options_with_sensor_ids_proto) {
  /** 分配一个新的轨迹ID */
  const int trajectory_id = trajectory_builders_.size();
  /** 为新的轨迹在轨迹构建器数组上分配空间，暂未初始化 */
  trajectory_builders_.emplace_back();
  /** 将传感器配置项压入轨迹配置项数组 */
  all_trajectory_builder_options_.push_back(options_with_sensor_ids_proto);
  /** 确认轨迹构建器数组和其配置项数组长度一致 */
  CHECK_EQ(trajectory_builders_.size(), all_trajectory_builder_options_.size());
  /** 返回新的轨迹id */
  return trajectory_id;
}

/**
 * @brief 完成轨迹
 * @param trajectory_id 轨迹id
 */
void MapBuilder::FinishTrajectory(const int trajectory_id) {
  /** 断开传感器与该轨迹的关系 */
  sensor_collator_->FinishTrajectory(trajectory_id);
  /** 在PoseGraph中关闭轨迹 */
  pose_graph_->FinishTrajectory(trajectory_id);
}

/**
 * @brief 根据指定的id查询submap，把查询的结果放到response中
 * @param trajectory_id submap的id
 * @param response 查询的结果
 */
std::string MapBuilder::SubmapToProto(
    const SubmapId& submap_id, proto::SubmapQuery::Response* const response) {
  /** 检查map的合法性 */
  if (submap_id.trajectory_id < 0 ||
      submap_id.trajectory_id >= num_trajectory_builders()) {
    return "Requested submap from trajectory " +
           std::to_string(submap_id.trajectory_id) + " but there are only " +
           std::to_string(num_trajectory_builders()) + " trajectories.";
  }
  /** 从PoseGraph中查询到submap的数据，并检查数据的有效性 */
  const auto submap_data = pose_graph_->GetSubmapData(submap_id);
  if (submap_data.submap == nullptr) {
    return "Requested submap " + std::to_string(submap_id.submap_index) +
           " from trajectory " + std::to_string(submap_id.trajectory_id) +
           " but it does not exist: maybe it has been trimmed.";
  }
  /** 将查询到的数据保存到response中 */
  submap_data.submap->ToResponseProto(submap_data.pose, response);
  return "";
}

/**
 * @brief 序列化（保存）所有状态数据
 * @param writer 输出流的指针
 */
void MapBuilder::SerializeState(io::ProtoStreamWriterInterface* const writer) {
  io::WritePbStream(*pose_graph_, all_trajectory_builder_options_, writer);
}

/**
 * @brief 反序列化（加载）所有状态数据
 * @param reader 输入流的指针
 * @param load_frozen_state 是否加载为frozen状态
 */
void MapBuilder::LoadState(io::ProtoStreamReaderInterface* const reader,
                           bool load_frozen_state) {
  /** 创建并初始化反序列化器 */
  io::ProtoStreamDeserializer deserializer(reader);

  /** 反序列化PoseGraph */
  // Create a copy of the pose_graph_proto, such that we can re-write the
  // trajectory ids.
  proto::PoseGraph pose_graph_proto = deserializer.pose_graph();
  /** 反序列化轨迹构建器选项 */
  const auto& all_builder_options_proto =
      deserializer.all_trajectory_builder_options();

  /** 逐一反序列化每个轨迹 */
  std::map<int, int> trajectory_remapping;
  for (auto& trajectory_proto : *pose_graph_proto.mutable_trajectory()) {
    const auto& options_with_sensor_ids_proto =
        all_builder_options_proto.options_with_sensor_ids(
            trajectory_proto.trajectory_id());
	/** 添加轨迹 */
    const int new_trajectory_id =
        AddTrajectoryForDeserialization(options_with_sensor_ids_proto);
	/** 添加轨迹到trajectory_remapping中 */
    CHECK(trajectory_remapping
              .emplace(trajectory_proto.trajectory_id(), new_trajectory_id)
              .second)
        << "Duplicate trajectory ID: " << trajectory_proto.trajectory_id();
	/** 分配轨迹的id */
    trajectory_proto.set_trajectory_id(new_trajectory_id);
	/** 轨迹加载为冻结状态 */
    if (load_frozen_state) {
      pose_graph_->FreezeTrajectory(new_trajectory_id);
    }
  }

  //FIXME：这个地方没看懂
  // Apply the calculated remapping to constraints in the pose graph proto.
  for (auto& constraint_proto : *pose_graph_proto.mutable_constraint()) {
    constraint_proto.mutable_submap_id()->set_trajectory_id(
        trajectory_remapping.at(constraint_proto.submap_id().trajectory_id()));
    constraint_proto.mutable_node_id()->set_trajectory_id(
        trajectory_remapping.at(constraint_proto.node_id().trajectory_id()));
  }

  /** 恢复每个轨迹的submap */
  MapById<SubmapId, transform::Rigid3d> submap_poses;
  for (const proto::Trajectory& trajectory_proto :
       pose_graph_proto.trajectory()) {
    for (const proto::Trajectory::Submap& submap_proto :
         trajectory_proto.submap()) {
      submap_poses.Insert(SubmapId{trajectory_proto.trajectory_id(),
                                   submap_proto.submap_index()},
                          transform::ToRigid3(submap_proto.pose()));
    }
  }

  /** 恢复节点的pose */
  MapById<NodeId, transform::Rigid3d> node_poses;
  for (const proto::Trajectory& trajectory_proto :
       pose_graph_proto.trajectory()) {
    for (const proto::Trajectory::Node& node_proto : trajectory_proto.node()) {
      node_poses.Insert(
          NodeId{trajectory_proto.trajectory_id(), node_proto.node_index()},
          transform::ToRigid3(node_proto.pose()));
    }
  }

  /** 恢复地标的位姿 */
  // Set global poses of landmarks.
  for (const auto& landmark : pose_graph_proto.landmark_poses()) {
    pose_graph_->SetLandmarkPose(landmark.landmark_id(),
                                 transform::ToRigid3(landmark.global_pose()));
  }

  /** 从输入流中反序列化各种对象 */
  SerializedData proto;
  while (deserializer.ReadNextSerializedData(&proto)) {
    switch (proto.data_case()) {
      case SerializedData::kPoseGraph:
        LOG(ERROR) << "Found multiple serialized `PoseGraph`. Serialized "
                      "stream likely corrupt!.";
        break;
      case SerializedData::kAllTrajectoryBuilderOptions:
        LOG(ERROR) << "Found multiple serialized "
                      "`AllTrajectoryBuilderOptions`. Serialized stream likely "
                      "corrupt!.";
        break;
      case SerializedData::kSubmap: {
        proto.mutable_submap()->mutable_submap_id()->set_trajectory_id(
            trajectory_remapping.at(
                proto.submap().submap_id().trajectory_id()));
        const transform::Rigid3d& submap_pose = submap_poses.at(
            SubmapId{proto.submap().submap_id().trajectory_id(),
                     proto.submap().submap_id().submap_index()});
        pose_graph_->AddSubmapFromProto(submap_pose, proto.submap());
        break;
      }
      case SerializedData::kNode: {
        proto.mutable_node()->mutable_node_id()->set_trajectory_id(
            trajectory_remapping.at(proto.node().node_id().trajectory_id()));
        const transform::Rigid3d& node_pose =
            node_poses.at(NodeId{proto.node().node_id().trajectory_id(),
                                 proto.node().node_id().node_index()});
        pose_graph_->AddNodeFromProto(node_pose, proto.node());
        break;
      }
      case SerializedData::kTrajectoryData: {
        proto.mutable_trajectory_data()->set_trajectory_id(
            trajectory_remapping.at(proto.trajectory_data().trajectory_id()));
        pose_graph_->SetTrajectoryDataFromProto(proto.trajectory_data());
        break;
      }
      case SerializedData::kImuData: {
        if (load_frozen_state) break;
        pose_graph_->AddImuData(
            trajectory_remapping.at(proto.imu_data().trajectory_id()),
            sensor::FromProto(proto.imu_data().imu_data()));
        break;
      }
      case SerializedData::kOdometryData: {
        if (load_frozen_state) break;
        pose_graph_->AddOdometryData(
            trajectory_remapping.at(proto.odometry_data().trajectory_id()),
            sensor::FromProto(proto.odometry_data().odometry_data()));
        break;
      }
      case SerializedData::kFixedFramePoseData: {
        if (load_frozen_state) break;
        pose_graph_->AddFixedFramePoseData(
            trajectory_remapping.at(
                proto.fixed_frame_pose_data().trajectory_id()),
            sensor::FromProto(
                proto.fixed_frame_pose_data().fixed_frame_pose_data()));
        break;
      }
      case SerializedData::kLandmarkData: {
        if (load_frozen_state) break;
        pose_graph_->AddLandmarkData(
            trajectory_remapping.at(proto.landmark_data().trajectory_id()),
            sensor::FromProto(proto.landmark_data().landmark_data()));
        break;
      }
      default:
        LOG(WARNING) << "Skipping unknown message type in stream: "
                     << proto.GetTypeName();
    }
  }

  /** 如果是加载冻结的轨迹 */
  if (load_frozen_state) {
    // Add information about which nodes belong to which submap.
    // Required for 3D pure localization.
    for (const proto::PoseGraph::Constraint& constraint_proto :
         pose_graph_proto.constraint()) {
      if (constraint_proto.tag() !=
          proto::PoseGraph::Constraint::INTRA_SUBMAP) {
        continue;
      }
      pose_graph_->AddNodeToSubmap(
          NodeId{constraint_proto.node_id().trajectory_id(),
                 constraint_proto.node_id().node_index()},
          SubmapId{constraint_proto.submap_id().trajectory_id(),
                   constraint_proto.submap_id().submap_index()});
    }
  } else {
    // When loading unfrozen trajectories, 'AddSerializedConstraints' will
    // take care of adding information about which nodes belong to which
    // submap.
    pose_graph_->AddSerializedConstraints(
        FromProto(pose_graph_proto.constraint()));
  }
  CHECK(reader->eof());
}

}  // namespace mapping
}  // namespace cartographer
