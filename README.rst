DASS: DEA ARD scene select
=======================================================


This code is used to select scenes to be processed to ARD (Analysis Ready Data). This Repo si used ot build a module to run at NCI.  It is used in production to generate Landsat and Sentinel 2 Collection 3 ARD.

---

*Note*: this repository uses [pre-commit](https://pre-commit.com/).

Please run

     pre-commit install

after cloning to make your life easier.

(if "pre-commit not found", then `pip install pre-commit` or `conda install pre_commit` and try again)

---

DASS Module creation
----------------------------
Modules are built off the master branch. To generate a new production module, follow these steps:


1. login or sudo as lpgs in a terminal since production modules must be built as the lpgs user
2. Get to the lpgs sandbox of this repo "cd /home/547/lpgs/sandbox/dea-ard-scene-select/module/"
3. Update to the latest version of master. Run "git pull --rebase"
4. build the new version of the package. Run "./go.sh --prod"
5. If there are no errors in the terminal, the package build should have been successful and the
final line will reflect where the newly built dea-ard-scene-select package has been written to.
    For example,
        "Wrote modulefile to /g/data/v10/private/modules/modulefiles/ard-scene-select-py3-dea/20231010"
6. tag the new version and push the tag up. This does not have to be done as lpgs. For example:

    git tag -a "ard-scene-select-py3-dea/20231010" -m "Add new integration tests"
    git push origin ard-scene-select-py3-dea/20231010
7. Test the new module by updating the test scripts.
8. To use the new module in production, update module parameters in the airflow dags; nci_s2_ard.py and
nci_ls_ard.py. These are in the airflow repo;
    https://bitbucket.org/geoscienceaustralia/dea-airflow/src/master/dags/nci_ard/
Note, update and test in the develop branch and then merge to master.


Updating the ard_pipeline Modules
---------------------------------
The ard_pipeline modules are used to process the ARD.
To update the ARD software used in production update the dass-prod-wagl-ls.env and dass-prod-wagl-s2.env files used by DASS.

These files are in the landsat-downloader repo;

   https://bitbucket.org/geoscienceaustralia/landsat-downloader/src/master/config/

Follow the steps in the readme of the landsat-downloader repo to update the env files used in production.

Testing
-------
There are a variety of tests in the tests directory.
Depending on what you want to test you may need to edit the scripts.
For example, to test a new DASS module it would need to be added to the script.
The scripts have been set up to load modules on the NCI.
Otherwise it is assumed the scripts are running in an appropriate environment.

For the integration tests, it is assumed that there is a  $USER"_automated_testing" database that can be created and destroyed.
For NCI the database is hosted on deadev.nci.org.au.

DASS unit tests
---------------
To run the unit tests, run the following from the tests directory:

    ./do_tests.sh

DASS integration tests
----------------------

Read this [README](tests/integration_tests/README.md)
Read this `README <tests/integration_tests/README.md>`_

Test that the modules work by doing a development run that produces and indexes ARD.
This is done from dea-ard-scene-select/tests/ard_tests by running:

    ./overall.sh

To check that the ARD processing was successful run check_db.sh and see that the number of scenes in the database has increased.




Code checker/validator
----------------------

  There is a utility, 'check_code.sh' which does the following in sequence:
  * ensures that our tests are passing (ie. runs all tests using pytest)
  * ensures consistency by applying our python code formatter across scripts, tests and scene_select directories
  * ensures code quality by running pylint across scrips, tests and scene_select directories

 To run this, one will just execute './check_code.sh'.
 It will provide a report when it finishes its execution.
