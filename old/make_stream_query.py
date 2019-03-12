#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""gaia_tools helper functions
Make constructing SQL queries for gaia_tools easier
See gaia_tools python package by Jo Bovy for details

By Nathaniel Starkman
"""

from gaiadr2.make_gaia_query import make_gaia_query  # FIXME paths!

##########################################

l0cols = """
--Rotation Matrix
{K00}*cos(radians(dec))*cos(radians(ra))+
{K01}*cos(radians(dec))*sin(radians(ra))+
{K02}*sin(radians(dec)) AS cosphi1cosphi2,

{K10}*cos(radians(dec))*cos(radians(ra))+
{K11}*cos(radians(dec))*sin(radians(ra))+
{K12}*sin(radians(dec)) AS sinphi1cosphi2,

{K20}*cos(radians(dec))*cos(radians(ra))+
{K21}*cos(radians(dec))*sin(radians(ra))+
{K22}*sin(radians(dec)) AS sinphi2,

--c1, c2
{sindecngp}*cos(radians(dec)){mcosdecngp:+}*sin(radians(dec))*cos(radians(ra{mrangp:+})) as c1,
{cosdecngp}*sin(radians(ra{mrangp:+})) as c2
"""

l1cols = """
gaia.cosphi1cosphi2, gaia.sinphi1cosphi2, gaia.sinphi2,
gaia.c1, gaia.c2,

atan2(sinphi1cosphi2, cosphi1cosphi2) AS phi1,
atan2(sinphi2, sinphi1cosphi2 / sin(atan2(sinphi1cosphi2, cosphi1cosphi2))) AS phi2"""

l2cols = """
gaia.sinphi1cosphi2, gaia.cosphi1cosphi2, gaia.sinphi2,
gaia.phi1, gaia.phi2,
gaia.c1, gaia.c2,

( c1*pmra+c2*pmdec)/cos(phi2) AS pmphi1,
(-c2*pmra+c1*pmdec)/cos(phi2) AS pmphi2"""

l3cols = """
gaia.phi1, gaia.phi2,
gaia.pmphi1, gaia.pmphi2"""

###########

l0sel = 'parallax < 1.'

l3sel = """
    phi1 > {phi1min:+}
AND phi1 < {phi1max:+}
AND phi2 > {phi2min:+}
AND phi2 < {phi2max:+}
--AND pmphi1 > {pm_phi1min} AND pmphi1 < {pm_phi1max}
--AND pmphi2 > {pm_phi2min} AND pmphi2 < {pm_phi2max}"""

##############


def make_streamsection_query(l3userasdict, random_index=1000):
    if random_index is None:
        pass
    else:
        newl0sel = l0sel + " AND gaia.random_index < {}".format(random_index)

    # Making Query
    query = make_gaia_query(
        gaia_mags=True, panstarrs1=True,
        user_cols=l3cols,
        use_AS=True,
        user_ASdict=l3userasdict,
        # Inner Query
        FROM=make_gaia_query(
            gaia_mags=True,
            user_cols=l2cols,
            # Inner Query
            FROM=make_gaia_query(
                gaia_mags=True,
                user_cols=l1cols,
                # Inner Query
                FROM=make_gaia_query(
                    gaia_mags=True,
                    inmostquery=True,
                    user_cols=l0cols,
                    WHERE=newl0sel))),
        WHERE=l3sel,
        ORDERBY='gaia.parallax'
    )
    return query
