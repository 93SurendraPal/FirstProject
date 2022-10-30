from ..essentials.tool_lib import adselect_ops,misc_functions
import sys

id = int(sys.argv[1]) 
cpccpa_flag = int(sys.argv[2])
type_flag = int(sys.argv[3]) # 4: adgroup, 3: campaign, 2: campaign_group

misc = misc_functions(id)
if type_flag==2:

        group_details = misc.campgroup_information_call()
        print group_details

elif type_flag == 3:

        group_details = misc.twitter_getadsets_info()

        


