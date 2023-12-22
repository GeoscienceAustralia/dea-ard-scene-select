# DEA ARD scene select integration testing

To run the integration tests, run the following from the integration_tests directory:

    ./launch_test.sh

The scripts have been set up to load modules on the NCI.
Otherwise it is assumed the scripts are running in an appropriate environment.

For the integration tests, it is assumed that there is a  $USER"_automated_testing" database that can be created and destroyed.
For NCI the database is hosted on deadev.nci.org.au.

The tests here are based on the requirements [here](https://geoscienceau.sharepoint.com/:w:/r/sites/DEACoreTeam/Shared%20Documents/Architecture%20Documents/ARD%20Pipelines/scene_select_requirements.docx?d=w413c9b1e36964be7b84142d725e00f6d&csf=1&web=1&e=eFkneA)

