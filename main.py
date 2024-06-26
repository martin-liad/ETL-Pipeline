from pathlib import Path
import sys

from jobs import GlobalEnv, JobEnv
from jobs.fingertips import FingertipsYouthOfferJob

def main() -> int:
    """
    Main workflow script, executes all ETL jobs in order.
    """

    # Config
    global_env = GlobalEnv(
        path_raw = Path("data/raw"),
        path_downloaded = Path("data/downloaded"),
        path_interim = Path("data/interim"),
        path_processed = Path("data/processed"),
    )

    # Run
    job_env = JobEnv(
        global_env=global_env,
        source_name="nhs_fingertips", 
        collection_name="youth_offer",
    )
    FingertipsYouthOfferJob().run(job_env, 
        indicator_names = [
            'Admissions for asthma (0 to 9 years)',
            'Admissions for diabetes (0 to 9 years)',
            'Admissions for epilepsy (0 to 9 years)',
        ])

    # All done    
    return 0

if __name__ == '__main__':
    sys.exit(main())