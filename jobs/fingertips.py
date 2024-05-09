import glob
import os
import pandas as pd

from jobs import JobEnv

import fingertips_py as ftp

class FingertipsYouthOfferJob:
    """
    Extracts indicators from NHS Fingertips that relate to the 
    Lewisham Youth Offer.
    """

    def run(self, job_env: JobEnv):
        
        indicator_names = [
            'Admissions for asthma (0 to 9 years)',
            'Admissions for diabetes (0 to 9 years)',
            'Admissions for epilepsy (0 to 9 years)',
        ]

        # Select sources for all desired indicators
        print("Fetching Fingertips metadata...")
        meta = ftp.metadata.get_metadata_for_all_indicators_from_csv() 
        indicators = meta[meta.Indicator.isin(indicator_names)].sort_values(by='Indicator')
        assert len(indicators) == len(indicator_names), (len(indicators), len(indicator_names))
        
        # Annotations for units -- this is not included in the basic metadata
        print("Fetching units of measurement...")
        full_meta = ftp.metadata.get_metadata_for_all_indicators(include_definition=True) 
        indicator_units = { 
            full_meta[_id]['IID']: full_meta[_id]['Unit']['Label'] 
                for _id in full_meta.keys() 
        }
        indicator_units[20401] # example

        # Get the full data set for each indicator --
        # we will filter and preprocess it later.
        os.makedirs(job_env.downloaded_path(), exist_ok = True)
        for iid in indicators['Indicator ID'].values:
            print(f"Fetching indicator {iid}...")
            data = ftp.get_data_for_indicator_at_all_available_geographies(iid)
            print(f"{len(data)} records.")
            
            # Annotate with units before saving to CSV -- 
            # they're not included in the basic data export
            units = indicator_units[iid]
            data['Unit Label'] = units

            csv_fn = f'{job_env.downloaded_path()}/fingertips-{iid}.csv'
            data.to_csv(csv_fn, index=False)
            print(f"Saved as {csv_fn}")

        # Any other preprocessing to produce the final output.
        os.makedirs(job_env.processed_path(), exist_ok = True)
        for fn in glob.glob(f'{job_env.downloaded_path()}/*.csv'):
            d = pd.read_csv(fn)

            # For exports
            filename_tag = d['Indicator Name'].unique()[0]

            # Filter Lewisham, core features only (skip any categories)
            d = d[d['Category Type'].isnull()] # Only top-level aggregates for now
            d = d[d['Area Name']=='Lewisham']  # Lewisham only
            d = d.drop(columns='Area Type').drop_duplicates() # To remove multiple redundant entries for diff area types
            d = d.dropna(subset='Value')       # These do occur in a few of the indicators

            if len(d)==0:
                print("No data left after filtering... skipping.")
                print()
                continue
            
            # Export
            csv_fn = f'{job_env.processed_path()}/Fingertips-{filename_tag}-Lewisham-core_features.csv'
            d.to_csv(csv_fn, index=False)
            print(f"Saved as {csv_fn}")
