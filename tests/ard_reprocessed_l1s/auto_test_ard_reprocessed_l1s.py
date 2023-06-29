##import pytest
##from flask.testing import FlaskClient
import os

# Append path to PYTHONPATH
additional_path = '/g/data/u46/users/gy5636/dea-ard-scene-select/'
os.environ['PYTHONPATH'] = os.pathsep.join([os.environ.get('PYTHONPATH', ''), additional_path])
print(f"The Path is {os.environ['PYTHONPATH']}")
# Import auto odc db from datacube
###from datacube-explorer.cubedash.testutils import database ## TODO - check to see this works
import sys
#sys.path.append('/g/data/u46/users/gy5636')
#print(f"The Path is {sys.path}")
# Append directory path to PYTHONPATH

import sys
from pathlib import Path

TEST_DATA_DIR = Path(__file__).parent.joinpath("..", "test_data", "ls9_reprocessing").resolve()
##SCRATCH_DIR = Path(__file__).parent.joinpath("scratch")

#current_base_path = REPROCESS_TEST_DIR

# # todo  MODIFY THESE!!!   -- tuesday - find the yaml file and inject them in... then call auto_odc


old_dir_06_27 = TEST_DATA_DIR.joinpath(
    "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
)
old_yaml_fname_06_27 = old_dir_06_27.joinpath(
    "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
)

new_base_path = TEST_DATA_DIR.joinpath("moved")

new_dir_06_21 = TEST_DATA_DIR.joinpath(
    "moved", "ga_ls9c_ard_3", "092", "081", "2022", "06", "21"
)
fname_06_21 = new_dir_06_21.joinpath(
    "ga_ls9c_ard_3-2-1_092081_2022-06-21_final.odc-metadata.yaml"
)

new_dir_06_27 = TEST_DATA_DIR.joinpath(
    "moved", "ga_ls9c_ard_3", "102", "076", "2022", "06", "27"
)
yaml_fname_06_27 = new_dir_06_27.joinpath(
    "ga_ls9c_ard_3-2-1_102076_2022-06-27_final.odc-metadata.yaml"
)
ard_id_06_27 = "d9a499d1-1abd-4ed1-8411-d584ca45de25"

METADATA_TYPES = [old_yaml_fname_06_27, fname_06_21,yaml_fname_06_27, ]
PRODUCTS = []
DATASETS = []


from cubedash.testutils.database import * # Gordon - todo -this is just a hack 


#
#
## Use the 'auto_odc_db' fixture to populate the database with sample data.
pytestmark = pytest.mark.usefixtures("auto_odc_db")
#
#

def test_datacube():
    # Assert the ODC location info is correct
    dc = datacube.Datacube(
        app="test_ard_reprocessed_l1s", config=str(os.getenv("DATACUBE_CONFIG_PATH"))
    )
    ard_dataset = dc.index.datasets.get(ard_id_06_27)
    local_path = Path(ard_dataset.local_path).resolve()
    assert str(local_path) == str(yaml_fname_06_27), "The OCD ARD path has been updated"


# TODO - Gordon - modify this with contents
#  from test_ard_reprocessed_l1s
# TODO - Gordon - take the contents / setup config from go_Test_ard_reprocessed_l1s.sh
# and plant them in here.  WE only want to use 1 script

#import cubedash
#from cubedash.summary import SummaryStore
#from integration_tests.asserts import get_html
#
#
######## Start
#from pathlib import Path
#from click.testing import CliRunner
#import os.path
#import uuid
#from subprocess import check_output, STDOUT
#import pytest
#import os
#import datacube
#
#
#
#
#@pytest.fixture
#def set_up_dirs_and_db():
#    setup_script = Path(__file__).parent.joinpath("db_index.sh")
#    cmd = [setup_script]
#    try:
#        cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()
#    except Exception as e:
#        print(e.output.decode())  # print out the stdout messages up to the exception
#        print(e)  # To print out the exception message
#    print("====================")
#    print(cmd_stdout)
#    print("====================")
#
#    # So the scene select call uses the correct DB
#    if "HOSTNAME" in os.environ and "gadi" in os.environ["HOSTNAME"]:
#        # Nobody call their system Brigadiers, ok.
#        end_tag = "_dev"
#    else:
#        end_tag = "_local"
#    os.environ["DATACUBE_ENVIRONMENT"] = f"{os.getenv('USER')}{end_tag}"
#    os.environ["DATACUBE_CONFIG_PATH"] = str(
#        Path(__file__).parent.joinpath("datacube.conf")
#    )
#
#
#
######### End
#
#
# # todo  MODIFY THESE!!! 
#METADATA_TYPES = ["", ]
#PRODUCTS = [
#    "",
#]
#DATASETS = [""]
#
#
## Use the 'auto_odc_db' fixture to populate the database with sample data.
#pytestmark = pytest.mark.usefixtures("auto_odc_db")
#
#
#@pytest.fixture()
#def app_configured_client(client: FlaskClient):
#    cubedash.app.config["CUBEDASH_INSTANCE_TITLE"] = "Development - ODC"
#    cubedash.app.config["CUBEDASH_SISTER_SITES"] = (
#        ("Production - ODC", "http://prod.odc.example"),
#        ("Production - NCI", "http://nci.odc.example"),
#    )
#    cubedash.app.config["CUBEDASH_HIDE_PRODUCTS_BY_NAME_LIST"] = [
#        "ls5_pq_scene",
#        "ls7_pq_scene",
#        "ls8_pq_scene",
#        "ls5_pq_legacy_scene",
#        "ls7_pq_legacy_scene",
#    ]
#    return client
#
#
#@pytest.fixture()
#def total_indexed_products_count(summary_store: SummaryStore):
#    return len(list(summary_store.index.products.get_all()))
#
#
#def test_instance_title(app_configured_client: FlaskClient):
#    html = get_html(app_configured_client, "/about")
#
#    instance_title = html.find(".instance-title", first=True).text
#    assert instance_title == "Development - ODC"
#
#
#def test_hide_products_audit_page_display(
#    app_configured_client: FlaskClient, total_indexed_products_count
#):
#    html = get_html(app_configured_client, "/audit/storage")
#    hidden_product_count = html.find("span.hidden-product-count", first=True).text
#    assert hidden_product_count == "5"
#
#    h2 = html.find("h2", first=True).text
#    indexed_product_count = html.find("span.indexed-product-count", first=True).text
#    assert indexed_product_count == str(total_indexed_products_count)
#    assert str(total_indexed_products_count - 5) in h2
#
#
#def test_hide_products_audit_bulk_dataset_display(
#    app_configured_client: FlaskClient, total_indexed_products_count
#):
#    html = get_html(app_configured_client, "/audit/dataset-counts")
#    hidden_product_count = html.find("span.hidden-product-count", first=True).text
#    assert hidden_product_count == "5"
#
#    h2 = html.find("h2", first=True).text
#    indexed_product_count = html.find("span.indexed-product-count", first=True).text
#    assert indexed_product_count == str(total_indexed_products_count)
#    assert str(total_indexed_products_count - 5) in h2
#
#
#def test_hide_products_product_page_display(
#    app_configured_client: FlaskClient, total_indexed_products_count
#):
#    html = get_html(app_configured_client, "/products")
#    hidden_product_count = html.find("span.hidden-product-count", first=True).text
#    assert hidden_product_count == "5"
#
#    h2 = html.find("h2", first=True).text
#    indexed_product_count = html.find("span.indexed-product-count", first=True).text
#    assert indexed_product_count == str(total_indexed_products_count)
#    assert str(total_indexed_products_count - 5) in h2
#
#    listed_product_count = html.find("tr.collapse-when-small")
#    assert len(listed_product_count) == (total_indexed_products_count - 5)
#
#
#def test_hide_products_menu_display(
#    app_configured_client: FlaskClient, total_indexed_products_count
#):
#    html = get_html(app_configured_client, "/about")
#
#    hide_products = html.find("#products-menu li a.configured-hide-product")
#    assert len(hide_products) == 5
#
#    products_hide_show_switch = html.find("a#show-hidden-product")
#    assert products_hide_show_switch
#
#    html = get_html(app_configured_client, "/products/dsm1sv10")
#    products = html.find(".product-selection-header a.option-menu-link")
#    assert total_indexed_products_count - len(products) == 5
#
#
#def test_sister_sites(app_configured_client: FlaskClient):
#    html = get_html(app_configured_client, "/about")
#
#    sister_instances = html.find("#sister-site-menu ul li")
#    assert len(sister_instances) == 2
#
#    for sister_instance in sister_instances:
#        assert (
#            "/about" in sister_instance.find("a.sister-link", first=True).attrs["href"]
#        )
#
#
#def test_sister_sites_request_path(app_configured_client: FlaskClient):
#    html = get_html(app_configured_client, "/products/ls5_fc_albers")
#
#    sister_instances = html.find("#sister-site-menu ul li")
#    assert len(sister_instances) == 2
#
#    for sister_instance in sister_instances:
#        assert (
#            "/products/ls5_fc_albers"
#            in sister_instance.find("a.sister-link", first=True).attrs["href"]
#        )
#
#    html = get_html(app_configured_client, "/products/ls5_fc_albers/datasets")
#
#    sister_instances = html.find("#sister-site-menu ul li")
#    assert len(sister_instances) == 2
#
#    for sister_instance in sister_instances:
#        assert (
#            "/products/ls5_fc_albers/datasets"
#            in sister_instance.find("a.sister-link", first=True).attrs["href"]
#        )
