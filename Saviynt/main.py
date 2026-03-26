from saviynt_modules import SaviyntModule
from saviynt_modules.connector_m365_events import M365EventsConnector
from saviynt_modules.trigger_m365_events import M365EventsTrigger

if __name__ == "__main__":
    module = SaviyntModule()
    module.register(SaviyntEventsConnector, "saviynt_events_connector")
    module.run()