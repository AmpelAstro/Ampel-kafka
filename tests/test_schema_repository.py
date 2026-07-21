import fastavro

from ampel.lsst.kafka.HttpSchemaRepository import HttpSchemaRepostory

DEFAULT_SCHEMA = {
    "type": "record",
    "doc": "sample avro alert schema v4.1",
    "name": "elasticc.v0_9.alert",
    "fields": [
        {"doc": "unique alert identifer", "name": "alertId", "type": "long"},
        {
            "name": "diaSource",
            "type": {
                "type": "record",
                "name": "elasticc.v0_9.diaSource",
                "fields": [
                    {"name": "diaSourceId", "type": "long"},
                    {"name": "ccdVisitId", "type": "long"},
                    {
                        "default": None,
                        "name": "diaObjectId",
                        "type": ["null", "long"],
                    },
                    {
                        "default": None,
                        "name": "parentDiaSourceId",
                        "type": ["null", "long"],
                    },
                    {"name": "midPointTai", "type": "double"},
                    {"name": "filterName", "type": "string"},
                    {"name": "ra", "type": "double"},
                    {"name": "decl", "type": "double"},
                    {"name": "psFlux", "type": "float"},
                    {"name": "psFluxErr", "type": "float"},
                    {"name": "snr", "type": "float"},
                    {
                        "default": None,
                        "name": "nobs",
                        "type": ["null", "float"],
                    },
                ],
            },
        },
        {
            "default": None,
            "name": "prvDiaSources",
            "type": [
                "null",
                {"type": "array", "items": "elasticc.v0_9.diaSource"},
            ],
        },
        {
            "default": None,
            "name": "prvDiaForcedSources",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "elasticc.v0_9.diaForcedSource",
                        "fields": [
                            {"name": "diaForcedSourceId", "type": "long"},
                            {"name": "ccdVisitId", "type": "long"},
                            {"name": "diaObjectId", "type": "long"},
                            {"name": "midPointTai", "type": "double"},
                            {"name": "filterName", "type": "string"},
                            {"name": "psFlux", "type": "float"},
                            {"name": "psFluxErr", "type": "float"},
                            {"name": "totFlux", "type": "float"},
                            {"name": "totFluxErr", "type": "float"},
                        ],
                    },
                },
            ],
        },
        {
            "default": None,
            "name": "prvDiaNondetectionLimits",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "elasticc.v0_9.diaNondetectionLimit",
                        "fields": [
                            {"name": "ccdVisitId", "type": "long"},
                            {"name": "midPointTai", "type": "double"},
                            {"name": "filterName", "type": "string"},
                            {"name": "diaNoise", "type": "float"},
                        ],
                    },
                },
            ],
        },
        {
            "default": None,
            "name": "diaObject",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "elasticc.v0_9.diaObject",
                    "fields": [
                        {"name": "diaObjectId", "type": "long"},
                        {
                            "doc": "diaObject provenance",
                            "name": "simVersion",
                            "type": ["null", "string"],
                        },
                        {"name": "ra", "type": "double"},
                        {"name": "decl", "type": "double"},
                        {
                            "default": None,
                            "name": "mwebv",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "mwebv_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "z_final",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "z_final_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_ellipticity",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_sqradius",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zspec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zspec_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q000",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q010",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q020",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q030",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q040",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q050",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q060",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q070",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q080",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q090",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q100",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_p50",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_Y",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_ra",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_dec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_snsep",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_Y",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_ellipticity",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_sqradius",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zspec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zspec_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q000",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q010",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q020",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q030",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q040",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q050",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q060",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q070",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q080",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q090",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q100",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_p50",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_Y",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_ra",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_dec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_snsep",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_Y",
                            "type": ["null", "float"],
                        },
                    ],
                },
            ],
        },
        {
            "default": None,
            "name": "cutoutDifference",
            "type": ["null", "bytes"],
        },
        {"default": None, "name": "cutoutTemplate", "type": ["null", "bytes"]},
    ],
}


def test_load_schema():
    schema = HttpSchemaRepostory.load_schema(
        "https://raw.githubusercontent.com/LSSTDESC/elasticc/c47fbd301b87f915c77ac0046d7845c68c306444/alert_schema/elasticc.v0_9.alert.avsc"
    )
    assert schema == fastavro.parse_schema(DEFAULT_SCHEMA)


def test_split():
    url = "https://raw.githubusercontent.com/LSSTDESC/elasticc/c47fbd301b87f915c77ac0046d7845c68c306444/alert_schema/elasticc.v0_9.alert.avsc"
    assert HttpSchemaRepostory.get_parts(url) == (
        "https://raw.githubusercontent.com/LSSTDESC/elasticc/c47fbd301b87f915c77ac0046d7845c68c306444/alert_schema/",
        "elasticc.v0_9.alert",
        ".avsc",
    )
