from airflow.plugins_manager import AirflowPlugin

from niddk_api.manager import aav1_package as niddk_api_admin_v1
from niddk_api.manager import aav2_package as niddk_api_admin_v2
from niddk_api.manager import aav3_package as niddk_api_admin_v3
from niddk_api.manager import aav4_package as niddk_api_admin_v4
from niddk_api.manager import aav5_package as niddk_api_admin_v5
from niddk_api.manager import aav6_package as niddk_api_admin_v6
from niddk_api.manager import blueprint as niddk_api_blueprint


class AirflowNIDDKPlugin(AirflowPlugin):
    name = "niddk_api"
    appbuilder_views = [niddk_api_admin_v1, niddk_api_admin_v2, niddk_api_admin_v3,
                        niddk_api_admin_v4, niddk_api_admin_v5, niddk_api_admin_v6]
    appbuilder_menu_items = []
    flask_blueprints = [niddk_api_blueprint]
