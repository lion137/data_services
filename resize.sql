@data.setter
def data(self, value, host=None):
    self._data = value or []
    if host:
        self.host = host

    self._selected_services = [s for s in self._data if s.get("selected")]
    self._render()

def _render(self):
    total = len(self._selected_services)
    self.link.text = f"Select services ({total})"

    if total == 0:
        self.repeating_panel_1.items = []
        self.more_link.visible = False
        self.raise_event("x-resized")
        return

    # decide which services to show
    if self._expanded:
        shown = self._selected_services
    else:
        shown = self._selected_services[:_MAX_SERVICES]

    self.repeating_panel_1.items = [{"service": s} for s in shown]

    # show more / show less link
    if not self._expanded and total > _MAX_SERVICES:
        hidden = total - _MAX_SERVICES
        self.more_link.visible = True
        self.more_link.text = f"Show {hidden} more ▾"
    elif self._expanded and total > _MAX_SERVICES:
        self.more_link.visible = True
        self.more_link.text = "Show less ▴"
    else:
        self.more_link.visible = False

    self.raise_event("x-resized")


def services_connected(self, **params):
    component = Services_connected()
    row = params["cell"].getRow()

    component.data = row.getData()["services"]
    component.host = row.getData()["host"]

    def changed(**event_args):
        row.update({"services": event_args["value"]})
        row.reformat()
        if self.validationFailed:
            self.validateRow(row)

    from anvil import js
    def resized(**event_args):
        js.window.setTimeout(lambda: row.reformat(), 0)

    component.set_event_handler("changed", changed)
    component.set_event_handler("x-resized", resized)
    return component