<mark>**Travis County 2026 Primary Tool Documentation**</mark>
===========================

This repository contains various scripts used for analyzing data related to the 2026 primary elections in Travis County, Texas.

These scripts can be used to analyze the published registered voter registration list, public voting roster, and other information.

- **`process_p26_travis_voter_rosters.py`** — <mark>Python script to process Excel voter rosters from Travis County Elections.</mark>

- **`process_p26_travis_registered_voters.py`** — <mark>Python script to load and process the registered voter list from Travis County and analyze the voter roster data previously processed by the `process_p26_travis_voter_rosters.py` script and stored in the shelve database 'VoterRosterDatabase.dat'.</mark>

- **`process_p26_sample_bbm_ballots.py`** — <mark>Python script to extract individual sample BBM ballots from a single PDF exported by the ES&S 6.3.0.0 EMS for an election.</mark>

- **`process_p26_test_desk_ballots.py`** — <mark>Python script to process Ballot Style sample ballot cards from an ES&S EMS.</mark>

- **`process_p26_ess_test_deck.py`** — <mark>Python script to extract individual ballot cards from an ES&S EMS test deck.</mark>

- **`process_p26_ballot_detail_listing_csv.py`** — <mark>Python script to process a ballot detail listing CSV file from an ES&S EMS.</mark>

- **`analyze_p26_datafiles.py`** — <mark>Python script to analyze P26 data files, which are CSV files with ballot details for each ballot style.</mark>
