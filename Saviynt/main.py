from saviynt_modules import SaviyntModule
from saviynt_modules.connector_saviynt import SaviyntEventsConnector

if __name__ == "__main__":
    module = SaviyntModule()
    module.register(SaviyntEventsConnector, "saviynt_events_connector")
    module.run()
