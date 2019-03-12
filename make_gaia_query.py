#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""gaia_tools helper functions
Make constructing SQL queries for gaia_tools easier
See gaia_tools python package by Jo Bovy for details

By Nathaniel Starkman
"""
import json
import os


def _make_query_defaults(fpath=None):
    """Make default values for query
    loads from file

    Inputs
    ------
    fpath: str (or None or dict)
        the filepath of the gaia query defaults
        if None, uses 'gaia_query_defaults.json'
        if dict, assumes the dictionary is correct and returns as is

    Dictionary Values
    -----------------
    gaia cols
    gaia mags
    panstarrs cols
    asdict
    units
    """

    if issubclass(fpath.__class__, dict):
        return fpath

    if fpath is None:
        dirname = os.path.dirname(__file__)
        fpath = os.path.join(dirname, 'gaia_query_defaults.json')

    with open(fpath, 'r') as file:
        defaults = json.load(file)

    defaults['gaia cols'] = "\n".join(defaults['gaia cols'])
    defaults['gaia mags'] = "\n".join(defaults['gaia mags'])
    defaults['panstarrs cols'] = "\n".join(defaults['panstarrs cols'])

    return defaults


def _make_query_SELECT(user_cols=None, use_AS=True,
                       all_columns=False, gaia_mags=False, panstarrs1=False,
                       query=None, defaults=None):
    """Makes the SELECT portion of a gaia query

    Inputs:
    -------
    user_cols: str (or None)
        Data columns in addition to default columns
        Default: None
        ex: "gaia.L, gaia.B,"
    use_AS: bool
        True will add 'AS __' to the data columns
        This is good for the outer part of the query
        so as to have convenient names in the output data table.
        Default: True
    all_columns: bool
        Whether to include all columns
        via ', *'
        Default: False
    gaia_mags: bool
        Whether to include Gaia magnitudes
        Default: False
    panstarrs1: bool
        Whether to include Panstarrs1 g,r,i,z magnitudes and INNER JOIN on Gaia
        Default: False
    _asdict: dict (or None)
        dictionary of diminutives for the colums
        Default: None
            Will generate a dictionary of defaults, listed below
        ex: {'parralax': ' AS prlx'}
    query: str (or None)
        experimental feature

    Returns:
    --------
    query: str
        the SELECT portion of a gaia query

    Exceptions:
    -----------
    Unknown


    DEFAULTS:
    --------
    In a Json-esque format.
    See gaia_query_defaults.json
    {
        "asdict": {
            "source_id": " AS id",
            "ref_epoch": "",
            "parallax": " AS prlx",
            "parallax_error": " AS prlx_err",
            "ra": "",
            "ra_error": " AS ra_err",
            "dec": "",
            "dec_error": " AS dec_err",
            "pmra": "",
            "pmra_error": " AS pmra_err",
            "pmdec": "",
            "pmdec_error": " AS pmdec_err",
            "radial_velocity": " AS rvel",
            "radial_velocity_error": " AS rvel_err",
            "L": "",
            "B": "",
            "ecl_lon": "",
            "ecl_lat": "",
            "phot_bp_mean_mag": " AS Gbp",
            "phot_bp_mean_flux_over_error": " AS Gbpfluxfracerr",
            "phot_rp_mean_mag": " AS Grp",
            "phot_rp_mean_flux_over_error": " AS Grpfluxfracerr",
            "phot_g_mean_mag": " AS Gg",
            "phot_g_mean_flux_over_error": " AS Ggfluxfracerr",
            "bp_rp": " AS Gbp_rp",
            "bp_g": " AS Gbp_g",
            "g_rp": " AS Gg_rp",
            "panstarrs_g_mean_psf_mag": " AS g",
            "panstarrs_g_mean_psf_mag_error": " AS g_err",
            "panstarrs_r_mean_psf_mag": " AS r",
            "panstarrs_r_mean_psf_mag_error": " AS r_err",
            "panstarrs_i_mean_psf_mag": " AS i",
            "panstarrs_i_mean_psf_mag_error": " AS i_err",
            "panstarrs_z_mean_psf_mag": " AS z",
            "panstarrs_z_mean_psf_mag_error": " AS z_err"
        },

        "gaia cols": '\n'.join([
            "gaia.source_id{source_id},",
            "gaia.ref_epoch{ref_epoch},",
            "gaia.parallax{parallax}, gaia.parallax_error{parallax_error},",
            "gaia.ra{ra}, gaia.ra_error{ra_error},",
            "gaia.dec{dec}, gaia.dec_error{dec_error},",
            "gaia.pmra{pmra}, gaia.pmra_error{pmra_error},",
            "gaia.pmdec{pmdec}, gaia.pmdec_error{pmdec_error},",
            "gaia.radial_velocity{radial_velocity},
             gaia.radial_velocity_error{radial_velocity_error},",
            "--Gaia DR2 alt coords:",
            "gaia.L{L}, gaia.B{B},",
            "gaia.ecl_lon{ecl_lon}, gaia.ecl_lat{ecl_lat}"
        ]),
        "gaia mags": '\n'.join([
            "gaia.phot_bp_mean_mag{phot_bp_mean_mag},",
            "gaia.phot_bp_mean_flux_over_error{phot_bp_mean_flux_over_error},",
            "gaia.phot_rp_mean_mag{phot_rp_mean_mag},",
            "gaia.phot_rp_mean_flux_over_error{phot_rp_mean_flux_over_error},",
            "gaia.phot_g_mean_mag{phot_g_mean_mag},",
            "gaia.phot_g_mean_flux_over_error{phot_g_mean_flux_over_error},",
            "gaia.bp_rp{bp_rp}, gaia.bp_g{bp_g}, gaia.g_rp{g_rp}"
        ]),
        "panstarrs cols": '\n'.join([
            "panstarrs1.g_mean_psf_mag{panstarrs_g_mean_psf_mag},
             panstarrs1.g_mean_psf_mag_error{panstarrs_g_mean_psf_mag_error},",
            "panstarrs1.r_mean_psf_mag{panstarrs_r_mean_psf_mag},
             panstarrs1.r_mean_psf_mag_error{panstarrs_r_mean_psf_mag_error},",
            "panstarrs1.i_mean_psf_mag{panstarrs_i_mean_psf_mag},
             panstarrs1.i_mean_psf_mag_error{panstarrs_i_mean_psf_mag_error},",
            "panstarrs1.z_mean_psf_mag{panstarrs_z_mean_psf_mag},
             panstarrs1.z_mean_psf_mag_error{panstarrs_z_mean_psf_mag_error}"
        ])
    }
    """

    ####################
    # Defaults

    defaults = _make_query_defaults(defaults)

    if use_AS is False:  # replace with blank dict with asdict keys
        defaults['asdict'] = {k: '' for k in defaults['asdict']}

    # Start new query if one not provided
    if query is None:
        query = ""

    ####################
    # Building Selection

    # SELECT
    query += '--Data Columns:\nSELECT\n--GaiaDR2 Columns:\n'
    query += defaults['gaia cols']

    if gaia_mags is True:
        query += ',\n--GaiaDR2 Magnitudes and Colors:\n'
        query += defaults['gaia mags']

    if all_columns is True:
        query += ",\n--All Columns:\n*"

    if panstarrs1 is True:
        query += ',\n--Adding PanSTARRS Columns:\n'
        query += defaults['panstarrs cols']

    ####################
    # (Possible) User Input

    # Replacing {} with _asdict
    query = query.format(**defaults['asdict'])

    if user_cols is None:
        query += '\n'
    elif not isinstance(user_cols, str):
        raise TypeError('user_sel is not a (str)')
    elif user_cols == '':
            query += '\n'
    else:
        query += ',\n\n--Custom Selection & Assignement:'
        if user_cols[:1] != '\n':
            user_cols = '\n' + user_cols
        if user_cols[-1] == ',':
            user_cols = user_cols[:-1]
        query += user_cols

    ####################
    # Return
    return query


def _make_query_FROM(FROM=None, inmostquery=False, _tab='    '):
    """Make the FROM portion of a gaia query

    INPUTS:
    -------
    FROM: str (or None)
        User input FROM (though should not have FROM in it)
        Default: None
            goes to 'gaiadr2.gaia_source'
        Useful for nesting queries as an inner query can be input here
    query: str (or None)
        The current query. Gets modified by this function

    RETURNS:
    --------
    query: str
        The query, with FROM added
    """
    # FROM
    if FROM is None:
        FROM = 'gaiadr2.gaia_source'
    else:
        # Tab level
        FROM = _tab + FROM
        FROM = FROM.replace('\n', '\n{tab}'.format(tab=_tab))

    if inmostquery is False:
        p1, p2 ='(\n', '\n)'
    else:
        p1, p2 ='', ''

    s = "\n".join((
            "\n",
            "--SOURCE:",
            "FROM {p1}{userfrom}{p2} AS gaia".format(
                userfrom=FROM, p1=p1, p2=p2)))

    return s


def _query_tab_level(query, tablevel, _tab='    '):
    """Add tab level to query
    indents the whoel query by the tab level

    INPUTS
    ------
    query: str
        the query
    tablevel: int
        tab * tablevel
        tab = _tab
    _tab: str
        the tab
        default: 4 spaces

    RETURNS
    -------
    query
    """
    # Tab level
    query = (_tab * tablevel) + query
    query = query.replace('\n', '\n{tab}'.format(tab=_tab * tablevel))
    return query


def _make_query_WHERE(WHERE):
    """
    """

    # query = '' if query is None else query
    # Selection
    s = "\n\n--Selections:\nWHERE"
    if WHERE[:1] != '\n':
        WHERE = '\n' + WHERE
    s += WHERE

    return s


def _make_query_ORDERBY(ORDERBY):
    # query = '' if query is None else query
    s ='\n\n--Ordering:\nORDER BY'
    if ORDERBY[:1] != '\n':
        ORDERBY = '\n' + ORDERBY
    s += ORDERBY

    return s


def make_gaia_query(WHERE=None, ORDERBY=None, user_cols=None,
                    FROM=None, use_AS=False, all_columns=False,
                    panstarrs1=False, gaia_mags=False, user_ASdict=None,
                    inmostquery=False, defaults=None, _tab='    ',
                    pprint=False):
    """Makes a whole Gaia query

    INPUTS
    ------
    WHERE: str (or None)
        ADQL `WHERE' argument
    ORDERBY: str (or None)
        ADQL `ORDER BY' argument
    user_cols: str (or None)
        Data columns in addition to default columns
        Default: None
        ex: "gaia.L, gaia.B,"
    FROM: str (or None)
        ADQL `FFROM' argument
    use_AS: bool
        True will add 'AS __' to the data columns
        This is good for the outer part of the query
        so as to have convenient names in the output data table.
        Default: True
    all_columns: bool
        Whether to include all columns
        via ', *'
        Default: False
    gaia_mags: bool
        Whether to include Gaia magnitudes
        Default: False
    panstarrs1: bool
        Whether to include Panstarrs1 g,r,i,z magnitudes and INNER JOIN on Gaia
        Default: False
    user_ASdict: dict (or None)
        dictionary containing `AS' arguments
    inmostquery: bool
        needed if in-most query and not providing a FROM
    defaults: str (or None or dict)
        the filepath (str) of the gaia query defaults
        if None, uses '/gaia_query_defaults.json'
        if dict, assumes the dictionary is correct and returns
        SEE DEFAULTS
    _tab: str
        the tab
        default: 4 spaces
    pprint: bool
        print the query

    Returns
    -------
    query: str

    Exceptions
    ----------
    Unknown

    DEFAULTS
    --------
    In a Json-esque format.
    See gaia_query_defaults.json
    {
        "asdict": {
            "source_id": " AS id",
            "ref_epoch": "",
            "parallax": " AS prlx",
            "parallax_error": " AS prlx_err",
            "ra": "",
            "ra_error": " AS ra_err",
            "dec": "",
            "dec_error": " AS dec_err",
            "pmra": "",
            "pmra_error": " AS pmra_err",
            "pmdec": "",
            "pmdec_error": " AS pmdec_err",
            "radial_velocity": " AS rvel",
            "radial_velocity_error": " AS rvel_err",
            "L": "",
            "B": "",
            "ecl_lon": "",
            "ecl_lat": "",
            "phot_bp_mean_mag": " AS Gbp",
            "phot_bp_mean_flux_over_error": " AS Gbpfluxfracerr",
            "phot_rp_mean_mag": " AS Grp",
            "phot_rp_mean_flux_over_error": " AS Grpfluxfracerr",
            "phot_g_mean_mag": " AS Gg",
            "phot_g_mean_flux_over_error": " AS Ggfluxfracerr",
            "bp_rp": " AS Gbp_rp",
            "bp_g": " AS Gbp_g",
            "g_rp": " AS Gg_rp",
            "panstarrs_g_mean_psf_mag": " AS g",
            "panstarrs_g_mean_psf_mag_error": " AS g_err",
            "panstarrs_r_mean_psf_mag": " AS r",
            "panstarrs_r_mean_psf_mag_error": " AS r_err",
            "panstarrs_i_mean_psf_mag": " AS i",
            "panstarrs_i_mean_psf_mag_error": " AS i_err",
            "panstarrs_z_mean_psf_mag": " AS z",
            "panstarrs_z_mean_psf_mag_error": " AS z_err"
        },

        "gaia cols": '\n'.join([
            "gaia.source_id{source_id},",
            "gaia.ref_epoch{ref_epoch},",
            "gaia.parallax{parallax}, gaia.parallax_error{parallax_error},",
            "gaia.ra{ra}, gaia.ra_error{ra_error},",
            "gaia.dec{dec}, gaia.dec_error{dec_error},",
            "gaia.pmra{pmra}, gaia.pmra_error{pmra_error},",
            "gaia.pmdec{pmdec}, gaia.pmdec_error{pmdec_error},",
            "gaia.radial_velocity{radial_velocity},
             gaia.radial_velocity_error{radial_velocity_error},",
            "--Gaia DR2 alt coords:",
            "gaia.L{L}, gaia.B{B},",
            "gaia.ecl_lon{ecl_lon}, gaia.ecl_lat{ecl_lat}"
        ]),
        "gaia mags": '\n'.join([
            "gaia.phot_bp_mean_mag{phot_bp_mean_mag},",
            "gaia.phot_bp_mean_flux_over_error{phot_bp_mean_flux_over_error},",
            "gaia.phot_rp_mean_mag{phot_rp_mean_mag},",
            "gaia.phot_rp_mean_flux_over_error{phot_rp_mean_flux_over_error},",
            "gaia.phot_g_mean_mag{phot_g_mean_mag},",
            "gaia.phot_g_mean_flux_over_error{phot_g_mean_flux_over_error},",
            "gaia.bp_rp{bp_rp}, gaia.bp_g{bp_g}, gaia.g_rp{g_rp}"
        ]),
        "panstarrs cols": '\n'.join([
            "panstarrs1.g_mean_psf_mag{panstarrs_g_mean_psf_mag},
             panstarrs1.g_mean_psf_mag_error{panstarrs_g_mean_psf_mag_error},",
            "panstarrs1.r_mean_psf_mag{panstarrs_r_mean_psf_mag},
             panstarrs1.r_mean_psf_mag_error{panstarrs_r_mean_psf_mag_error},",
            "panstarrs1.i_mean_psf_mag{panstarrs_i_mean_psf_mag},
             panstarrs1.i_mean_psf_mag_error{panstarrs_i_mean_psf_mag_error},",
            "panstarrs1.z_mean_psf_mag{panstarrs_z_mean_psf_mag},
             panstarrs1.z_mean_psf_mag_error{panstarrs_z_mean_psf_mag_error}"
        ])
    }
    """

    query = _make_query_SELECT(user_cols=user_cols, use_AS=use_AS,
                               all_columns=all_columns, gaia_mags=gaia_mags,
                               panstarrs1=panstarrs1, defaults=defaults)

    query += _make_query_FROM(FROM, inmostquery=inmostquery, _tab=_tab)

    # Joining ON Panstarrs1
    if panstarrs1 is True:
        query += "\n".join((
            "\n",
            "--Comparing to PanSTARRS1",
            "INNER JOIN gaiadr2.panstarrs1_best_neighbour AS panstarrs1_match "
            "ON panstarrs1_match.source_id = gaia.source_id",
            "INNER JOIN gaiadr2.panstarrs1_original_valid AS panstarrs1 "
            "ON panstarrs1.obj_id = panstarrs1_match.original_ext_source_id"
            ""))

    # Adding WHERE
    if WHERE is not None:
        query += _make_query_WHERE(WHERE)
    # Adding ORDERBY
    if ORDERBY is not None:
        query += _make_query_ORDERBY(ORDERBY)
    # user_ASdict
    if user_ASdict is not None:
        query = query.format(**user_ASdict)

    # Query tab level
    # query = _query_tab_level(query, tablevel=tablevel)
    # # Finishing ADQL query
    # query += ";"

    # Returning
    if pprint is True:
        print(query)
    return query


def make_simple_gaia_query(WHERE=None, ORDERBY=None, user_cols=None,
                           FROM=None, all_columns=False,
                           panstarrs1=False, gaia_mags=False, user_ASdict=None,
                           defaults=None,
                           pprint=False):
    """make_gaia_query wrapper for single-layer queries
    with some defaults changed and options removed
    use_AS and inmostquery are now True.
    _tab is set to default
    """

    return make_gaia_query(WHERE=WHERE, ORDERBY=ORDERBY, user_cols=user_cols,
                           FROM=FROM, use_AS=True, all_columns=all_columns,
                           panstarrs1=panstarrs1, gaia_mags=gaia_mags,
                           user_ASdict=user_ASdict, inmostquery=True,
                           defaults=defaults, pprint=pprint)
