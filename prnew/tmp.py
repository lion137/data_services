def services_table_click(self, **event_args):
    selected_row = event_args["cell"].get_data()
    clicked_field = event_args["cell"].get_data()

    selected_row["selected_row"]["has_previous_summary"] = True

    if (
        clicked_field == "last_attestation"
        and selected_row["last_attestation"] is not None
    ):
        self.open_form_safely(
            "forms.summary_offline", data=selected_row["db_service"]
        )
        return

    self.open_form_safely("forms.Server_estate", data=selected_row["db_Service"])