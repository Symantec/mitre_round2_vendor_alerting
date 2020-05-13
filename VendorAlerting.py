import json
import pandas as pd
from pandas.io.json import json_normalize
import glob
import os


# Apply a score to every Alert event depending on what it was alerting on.
def alert_score_per_detection(modifiers, technique_weight):
    
    # If this is a critical technique, then multiple the detection score with a +1, else -1.
    sev_weight_multiplier = 0
    if technique_weight > 3:
        sev_weight_multiplier = 1
    else:
        sev_weight_multiplier = -1
    
    # We're only looking at Alerts and only those from pre-test configurations. 
    det_score = 0
    if ("Alert" in modifiers) and ("Configuration Change" not in modifiers):
        det_score = 3

    return det_score * sev_weight_multiplier


# Expand the nested JSON into a dataframe for further analysis
def expand_detection_categories(df):
    list_steps = []

    for i in range(df.shape[0]):
        step_parts = df.iloc[i]['SubStep'].split('.')
        formatted_step_id = "{}.{}.{}".format(step_parts[0].zfill(2), step_parts[1], step_parts[2].zfill(2))
        sub_step = formatted_step_id
        tech_name = df.iloc[i]['TechniqueName']
        tech_id = df.iloc[i]['TechniqueId']
        vendor = df.iloc[i]['Vendor']
        for detcat_list_item in df.iloc[i]['Detections']:
            expanded_detcat = {}
            expanded_detcat['Step'] = sub_step
            expanded_detcat['TechniqueName'] = tech_name
            expanded_detcat['TechniqueId'] = tech_id
            expanded_detcat['Vendor'] = vendor
            expanded_detcat['Category'] = detcat_list_item['DetectionType']
            if (len(detcat_list_item['Modifiers']) > 0):
                expanded_detcat['Modifiers'] = ','.join(detcat_list_item['Modifiers'])
            else:
                expanded_detcat['Modifiers'] = 'None'
            list_steps.append(expanded_detcat)

    return pd.DataFrame(list_steps)
	
	
	
if __name__ == '__main__':
	
	list_of_vendor_dataframes = []

	# Download the vendor results to a folder called "VendorResults"
	for infile in sorted(glob.glob(os.path.join('.\\VendorResults\\', '*json'))):
		filename = infile.split('\\', 2)[2]
		vendor_name = filename.split('.', 1)[0]
		
		d = None
		with open(infile) as f:
			d = json.load(f)

		df_cur_vendor_expanded_steps = pd.DataFrame()
		# We can expand one layer easily with json_normalize. Need to further expand below.
		df_cur_vendor_expanded_steps = json_normalize(data=d['Techniques'], record_path='Steps', meta=['TechniqueId', 'TechniqueName'])
		df_cur_vendor_expanded_steps['Vendor'] = vendor_name
		
		df_cur_vendor_expanded_detections = expand_detection_categories(df_cur_vendor_expanded_steps)
		
		list_of_vendor_dataframes.append(df_cur_vendor_expanded_detections.copy())
		
		
	df_all_vendor_results = pd.concat(list_of_vendor_dataframes)
	df_all_vendor_results = df_all_vendor_results[~df_all_vendor_results.Category.isin(["N/A"])]


	# Defined weights for each technique: 1-5 scale where 5 has the highest severity and 1 has least.
	df_weights = pd.read_excel('./Technique_Weighting.xlsx')
	df_all_vendor_results = pd.merge(df_all_vendor_results, df_weights[['TechniqueId', 'TechniqueWeight']], on="TechniqueId")

	df_all_vendor_results['Noise Score'] = df_all_vendor_results.apply(lambda row: alert_score_per_detection(row.Modifiers, row.TechniqueWeight), axis=1)

df_noise_score_detections = df_all_vendor_results.groupby('Vendor')['Noise Score'].sum().reset_index(name='Noise Score (Det)')
df_noise_score_detections.set_index('Vendor', inplace=True)
print(df_noise_score_detections.sort_values("Noise Score (Det)", ascending=False))
