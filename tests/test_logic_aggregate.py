from ftm_lakehouse.logic.entities.aggregate import aggregate_unsafe


def _caption(props: dict[str, str]) -> str:
    rows = [
        {
            "entity_id": "jane",
            "schema": "Person",
            "dataset": "test",
            "prop": p,
            "value": v,
        }
        for p, v in props.items()
    ]
    (payload,) = aggregate_unsafe(iter(rows), "test")
    return payload.to_dict()["caption"]


def test_caption_first_caption_prop():
    # `email` comes after `name` in `Person.caption`
    assert _caption({"name": "Jane Doe", "email": "jane@example.org"}) == "Jane Doe"
    assert _caption({"email": "jane@example.org"}) == "jane@example.org"
    assert _caption({"nationality": "de"}) == "Person"


def test_to_dict_does_not_depend_on_statement_order():
    """An entity's statements come in no fixed order; its dict is the same
    whatever the order – values and keys sorted, the caption their first."""
    rows = [
        {"entity_id": "e", "schema": "Person", "dataset": "t", "prop": p, "value": v}
        for p, v in (
            ("name", "Zoe"),
            ("name", "Ann"),
            ("country", "fr"),
            ("country", "de"),
            ("email", "a@example.org"),
        )
    ]
    dicts = [
        next(aggregate_unsafe(iter(order), "t")).to_dict()
        for order in (rows, rows[::-1], rows[2:] + rows[:2])
    ]
    assert dicts[0] == dicts[1] == dicts[2]
    assert list(dicts[0]["properties"]) == ["country", "email", "name"]
    assert dicts[0]["properties"]["name"] == ["Ann", "Zoe"]
    assert dicts[0]["caption"] == "Ann"
