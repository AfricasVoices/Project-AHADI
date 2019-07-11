# Project-AHADI
Data pipeline for AHADI.

This pipeline fetches all project data from a Rapid Pro instance, and processes it to produce CSV files suitable
for downstream analysis.

## Pre-requisites
Before the pipeline can be run, the following tools must be installed:
 - Docker
 - Bash
 
Development requires the following additional tools:
 - Python 3.6+
 - pipenv
 - git

## Usage
A pipeline run consists of the following five steps, executed in sequence:
(1) Download coded data from Coda (optional)
(2) Fetch all the relevant data from Rapid Pro
(3) Process the raw data to produce the outputs required for coding and then for analysis
(4) Upload the new data to Coda for manual verification and coding
(5) Back-up the project data root (optional)

To simplify the configuration and execution of these stages, this project includes a `run_scripts`
directory, which contains shell scripts for driving each of those stages. 

To run the entire pipeline, see [Run All Pipeline Stages](#run-all-pipeline-stages).

To run the above stages individually, see [these sections](#1-download-coded-data-from-coda)

### Run All Pipeline Stages
To run all the pipeline stages at once, and create a compressed backup of the data directory after the run,
run the following command from the `run_scripts` directory:

```
$ ./run_pipeline.sh <user> <pipeline-configuration-file-path> <coda-pull-auth-file> <coda-push-auth-file> <google-cloud-credentials-file-path> <coda-tools-root> <data-root> <data-backup-dir>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org` 
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `coda-pull-auth-file` is an absolute path to the private credentials file for the Coda instance to download manually coded datasets from.
- `coda-push-auth-file` is an absolute path to the private credentials file for the Coda instance to upload datasets to be manually coded to.
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `coda-tools-root` is an absolute path to a local directory containing a clone of the 
  [CodaV2](https://github.com/AfricasVoices/CodaV2) repository.
  If the given directory does not exist, the latest version of the Coda V2 repository will be cloned and set up 
  in that location automatically.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
- `data-backup-dir` is a directory which the `data-root` directory will be backed-up to after the rest of the
  pipeline stages have completed. The data is gzipped and given the name `data-<utc-date-time-now>-<git-HEAD-hash>`

### 1. Download Coded Data from Coda
This stage downloads coded datasets for this project from Coda (and is optional if manual coding hasn't started yet).
To use, run the following command from the `run_scripts` directory: 

```
$ ./1_coda_get.sh <coda-auth-file> <coda-tools-root> <data-root>
```

where:
- `coda-auth-file` is an absolute path to the private credentials file for the Coda instance to download coded datasets from.
- `coda-tools-root` is an absolute path to a local directory containing a clone of the 
  [CodaV2](https://github.com/AfricasVoices/CodaV2) repository.
  If the given directory does not exist, the latest version of the Coda V2 repository will be cloned and set up 
  in that location automatically.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  Downloaded Coda files are saved to `<data-root>/Coded Coda Files/<dataset>.json`.

### 2. Fetch Raw Data
This stage fetches all the raw data required by the pipeline from Rapid Pro.
To use, run the following command from the `run_scripts` directory:

```
$ ./2_fetch_raw_data.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <data-root>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org` 
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  Raw data will be saved to TracedData JSON files in `<data-root>/Raw Data`.

### 3. Generate Outputs
This stage processes the raw data to produce outputs for ICR, Coda, and messages/individuals/production
CSVs for final analysis.
To use, run the following command from the `run_scripts` directory:

```
$ ./3_generate_outputs.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <data-root>
```

where:
- `user` is the identifier of the person running the script, for use in the TracedData Metadata 
  e.g. `user@africasvoices.org`.
- `google-cloud-credentials-file-path` is an absolute path to a json file containing the private key credentials
  for accessing a cloud storage credentials bucket containing all the other project credentials files.
- `pipeline-configuration-file-path` is an absolute path to a pipeline configuration json file.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  All output files will be saved in `<data-root>/Outputs`.
   
As well as uploading the messages, individuals, and production CSVs to Drive (if configured in the 
pipeline configuration json file), this stage outputs the following files to `<data-root>/Outputs`:
 - Local copies of the messages, individuals, and production CSVs (`csap_s02_messages.csv`, `csap_s02_individuals.csv`, 
   `csap_s02_production.csv`)
 - A serialized export of the list of TracedData objects representing all the data that was exported for analysis 
   (`traced_data.json`)
 - For each week of radio shows, a random sample of 200 messages that weren't classified as noise, for use in ICR (`ICR/`)
 - Coda V2 messages files for each dataset (`Coda Files/<dataset>.json`). To upload these to Coda, see the next step.

### 4. Upload Auto-Coded Data to Coda
This stage uploads messages to Coda for manual coding and verification.
Messages which have already been uploaded will not be added again or overwritten.
To use, run the following command from the `run_scripts` directory:

```
$ ./4_coda_add.sh <coda-auth-file> <coda-tools-root> <data-root>
```

where:
- `coda-auth-file` is an absolute path to the private credentials file for the Coda instance to download coded datasets from.
- `coda-tools-root` is an absolute path to a local directory containing a clone of the 
  [CodaV2](https://github.com/AfricasVoices/CodaV2) repository.
  If the given directory does not exist, the latest version of the Coda V2 repository will be cloned and set up 
  in that location automatically.
- `data-root` is an absolute path to the directory in which all pipeline data should be stored.
  Downloaded Coda files are saved to `<data-root>/Coded Coda Files/<dataset>.json`.

### 5. Back-up the Data Directory
This stage makes a backup of the project data directory by creating a compressed, versioned, time-stamped copy at the
requested location.
To use, run the following command from the `run_scripts` directory:

```
$ ./5_backup_data_root.sh <data-root> <data-backups-dir>
```

where:
- `data-root` is an absolute path to the directory to back-up.
- `data-backups-dir` is a directory which the `data-root` directory will be backed-up to.
  The data is gzipped and given the name `data-<utc-date-time-now>-<git-HEAD-hash>`.


## Development

### CPU Profiling
To run the main processing stage with statistical cpu profiling enabled, pass the argument 
`--profile-cpu <cpu-profile-output-file>` to `run_scripts/3_generate_outputs.sh`.
The output file is generated by the statistical profiler [Pyflame](https://github.com/uber/pyflame), and is in a 
format compatible suitable for visualisation using [FlameGraph](https://github.com/brendangregg/FlameGraph).

### Memory Profiling
To run the main processing stage with memory profiling enabled, pass the argument
`--profile-memory <memory-profile-output-file>` to `run_scripts/3_generate_outputs.sh`.
The output file lists (memory usage, timestamp) pairs, sampled approximately every 0.1s.

To plot a graph of memory usage over time, on the host machine run:
```
# Install dependencies
$ pip install -U memory_profiler
$ pip install matplotlib

# Plot graph
$ mprof plot <path-to-memory-profile-file>
```

To annotate the graph with when specific functions were entered and exited, add `@profile` to 
the functions of interest. There is no need to import anything.

For full details on the memory profiler, see its [documentation page](https://pypi.org/project/memory-profiler/).
