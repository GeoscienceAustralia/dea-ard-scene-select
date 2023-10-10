ard-scene-select: select scenes to be processed by ARD
=======================================================


This code is used to select scenes to be processed by ARD. This Repo is deployed as a module to run at NCI.  It is used in production to generate Landsat collection 3 ARD.

Branch Structure
^^^^^^^^^^^^^^^^^^^^^^^^^

.. csv-table:: Branches
   :header: "Branch name", "Use"

   "master", "Stable code base"
   "production", "The branch used in production. It is the .env files that are used in production."
   "module-prod", "The branch used to produce a module."

---

*Note*: this repository uses [pre-commit](https://pre-commit.com/).

Please run

     pre-commit install

after cloning to make your life easier, automatically!

(if "pre-commit not found", then `pip install pre-commit` or `conda install pre_commit` and try again)

---

Scene Select Module creation
----------------------------
Modules are built off the module-prod branch. Create an annotated tag to tag a module build.

1. make sure the latest changes in the master branch are brought/sync-ed to the module-prod branch. To do this, create a PR from the master to the module-prod branch
2. once the PR is approved, merge it to the module-prod 
3. login or sudo as lpgs in a terminal since production modules must be built as the lpgs user
4. head to the path, "/home/547/lpgs/sandbox/dea-ard-scene-select/module". Run "cd /home/547/lpgs/sandbox/dea-ard-scene-select/module/"
5. checkout the "module-prod" branch. Run "git checkout module-prod"
6. tag the new version. This does not have to be done as lpgs. For example:

      git tag -a "ard-scene-select-py3-dea/20231010" -m "DSNS 262-baked pytest into scene select so that the new integration tests are supported"

7. build the new version of the package. Run "./go.sh --prod"
8. If there are no errors in the terminal, the package build should be have been successful and the
final line which resembles the example below will disclose where the newly built dea-ard-scene-select package as been built and written to.

"Wrote modulefile to /g/data/v10/private/modules/modulefiles/ard-scene-select-py3-dea/20231010"


Updating the ard_pipeline Modules
---------------------------------
To update the ard_pipeline modules update the .env files in;

    dea-ard-scene-select/scripts/prod/ard_env

Test that the modules work by doing a development run that produces and indexes ARD.
This is done from dea-ard-scene-select/tests/ard_tests by running:

    ./overall.sh

To check that the ARD processing was successful run check_db.sh

If this is successful then update the production branch with the new .env files.

To update the .env files used in production manually git pull from the production branch at this location;

   /g/data/v10/projects/c3_ard/dea-ard-scene-select/

Do this from the lpgs user account.


Code checker/validator
----------------------

  There is a utility, 'check_code.sh' which does the following in sequence:
  * ensures that our tests are passing (ie. runs all tests using pytest)
  * ensures consistency by applying our python code formatter across scripts, tests and scene_select directories
  * ensures code quality by running pylint across scrips, tests and scene_select directories

 To run this, one will just execute './check_code.sh'. 
 It will provide a report when it finishes its execution.
 
