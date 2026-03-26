from sekoia_automation.module import Module

from saviynt_modules.models import SaviyntConfiguration


class SaviyntModule(Module):
    configuration: SaviyntConfiguration
