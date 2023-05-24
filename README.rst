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

Module creation
---------------
Modules are built off the module-prod branch. Create an annotated tag to tag a module build.

e.g.

    git tag -a "ard-scene-select-py3-dea/20201126" -m "my version 20201126"

Production modules can must be built as the lpgs user. This can be done using this sandbox;


    /home/547/lpgs/sandbox/dea-ard-scene-select

To produce the module run this script;

    dea-ard-scene-select/modules/go.sh

Change `echo "dea_module_dir = ${dea_module_dir:=/g/data/v10/public/modules}"` to change the destination location of the module.


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
 
