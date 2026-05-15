def parse_month_range(start_month: str, end_month: str) -> list[tuple[int, int]]:
    """
    Parse "YYYY-MM" start/end strings into a list of (year, month) int tuples,
    inclusive on both ends.
    Example: parse_month_range("2024-01", "2024-03") -> [(2024, 1), (2024, 2), (2024, 3)]
    """
    for label, raw_value in (("start_month", start_month), ("end_month", end_month)):
        if (
            len(raw_value) != 7
            or raw_value[4] != "-"
            or not raw_value[:4].isdigit()
            or not raw_value[5:].isdigit()
        ):
            raise ValueError(
                f"{label} must use YYYY-MM format: {label}={raw_value!r}"
            )

    try:
        start_year_text, start_month_text = start_month.split("-")
        end_year_text, end_month_text = end_month.split("-")
        start_year = int(start_year_text)
        start_month_number = int(start_month_text)
        end_year = int(end_year_text)
        end_month_number = int(end_month_text)
    except ValueError as error:
        raise ValueError(
            "Month parameters must use YYYY-MM format: "
            f"start_month={start_month!r}, end_month={end_month!r}"
        ) from error

    for label, value in (
        ("start_month", start_month_number),
        ("end_month", end_month_number),
    ):
        if value < 1 or value > 12:
            raise ValueError(f"{label} month must be between 01 and 12: {value:02d}")

    start_value = (start_year, start_month_number)
    end_value = (end_year, end_month_number)
    if start_value > end_value:
        raise ValueError(
            f"start_month must be <= end_month: "
            f"start_month={start_month}, end_month={end_month}"
        )

    current_year, current_month = start_value
    month_range = []
    while (current_year, current_month) <= end_value:
        month_range.append((current_year, current_month))
        current_month += 1
        if current_month == 13:
            current_year += 1
            current_month = 1

    return month_range
