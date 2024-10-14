import structlog
from datacube.model import Range

from scene_select.bulk_process import matches_software_expressions


def test_matches_software_expressions():
    log = structlog.get_logger()
    assert matches_software_expressions(
        dict(wagl="1.2.3", fmask="4.2.0"),
        dict(wagl_version="1.2.3", fmask_version="4.2.0"),
        log,
    )
    assert not matches_software_expressions(
        dict(wagl="1.2.3", fmask="4.2.0"),
        dict(wagl_version="1.2.4", fmask_version="4.2.0"),
        log,
    )

    # lowest bound of range is inclusive
    assert matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.3", None)),
        log,
    )
    assert not matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.4", None)),
        log,
    )
    # highest bound is inclusive too
    assert matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.2", "1.2.3")),
        log,
    )
    assert matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range("1.2.3", "1.2.4")),
        log,
    )
    assert matches_software_expressions(
        dict(wagl="1.2.3"),
        dict(wagl_version=Range(None, "1.2.4")),
        log,
    )

