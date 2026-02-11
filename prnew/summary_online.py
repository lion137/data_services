# forms.Summary_offline.py - Anvil Forms Code

from ._anvil_designer import Summary_offlineTemplate


class Summary_offline(Summary_offlineTemplate, BaseModelForm):
    def __init__(self, data=None, **properties):
        self.init_components(**properties)
        self.init()
        
        self.data = data
        
        self.define_table_columns()
        self.load_data()

    def previous_click(self, **event_args):
        self.open_form_safely("forms.Recertification_home")

    def define_table_columns(self):
        self.tabulator_connections.columns = [
            {"title": "Direction", "field": "direction", "width": 120},
            {"title": "Attested Service Host type", "field": "destination", "width": 200},
            {
                "title": "Connection",
                "field": "connection",
                "formatter": self.service_condensed,
            },
            {"title": "Connection Type", "field": "type", "width": 130},
            {"title": "Required", "field": "required", "width": 130},
            {"title": "Reason", "field": "reason"},
        ]
        
        self.tabulator_server_estate.columns = [
            {"title": "Host", "field": "o_host"},
            {"title": "Environment", "field": "o_environment"},
            {"title": "Unicorn Lifecycle State", "field": "o_u_lifecyclestate"},
            {"title": "Shared server", "field": "m_o_shared_server"},
        ]
        
        server_estate_table_height = int(window.innerHeight * SERVER_ESTATE_TABLE_HEIGHT)
        connections_table_height = int(window.innerHeight * CONNECTIONS_TABLE_HEIGHT)
        
        self.tabulator_server_estate.options = {
            "maxHeight": f"{server_estate_table_height}px",
            "height": f"{server_estate_table_height}px",
            "selectableRows": False,
            "selectable": False,
            "progressiveLoad": "scroll",
            "progressiveLoadScrollMargin": 100,
        }
        
        self.tabulator_connections.options = {
            "maxHeight": f"{connections_table_height}px",
            "height": f"{connections_table_height}px",
            "selectableRows": False,
            "selectable": False,
            "progressiveLoad": "scroll",
            "progressiveLoadScrollMargin": 100,
        }

    def service_condensed(self, **params):
        row = params["cell"].getRow().getData()
        component = Service_condensed()
        component.service = row
        return component

    def load_data(self):
        if self.data is not None:
            ba_id = self.data.ba_id
            # TODO: Combine these two server calls into one (get_all_connections generates both)
            connections = anvil.server.call("get_all_connections", ba_id)
            server_estate = anvil.server.call("get_server_estate", ba_id)
            
            if connections:
                self.tabulator_connections.data = connections
            else:
                self.tabulator_connections.visible = False
                self.no_connections.visible = True
            
            if server_estate:
                self.tabulator_server_estate.data = server_estate
            else:
                self.tabulator_server_estate.visible = False
                self.no_server_estate.visible = True