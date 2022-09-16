import rospy
from home_robot.ros.grasp_helper import GraspClient

rospy.init_node('test_grasping')
p = "stretch_2022_09_14-12_51_59.npy"
segmap, rgb, depth, cam_K, pc_full, pc_colors = load_available_input_data(p, K=None)

print(segmap)
print(pc_full)
print(pc_colors)

client = GraspClient()

client.request(pc_full, pc_colors, segmap)
