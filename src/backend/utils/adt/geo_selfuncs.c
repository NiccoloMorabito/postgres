/*-------------------------------------------------------------------------
 *
 * geo_selfuncs.c
 *	  Selectivity routines registered in the operator catalog in the
 *	  "oprrest" and "oprjoin" attributes.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/geo_selfuncs.c
 *
 *	XXX These are totally bogus.  Perhaps someone will make them do
 *	something reasonable, someday.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/geo_decls.h"

#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/rangetypes.h"

/*
 *	Selectivity functions for geometric operators.  These are bogus -- unless
 *	we know the actual key distribution in the index, we can't make a good
 *	prediction of the selectivity of these operators.
 *
 *	Note: the values used here may look unreasonably small.  Perhaps they
 *	are.  For now, we want to make sure that the optimizer will make use
 *	of a geometric index if one is available, so the selectivity had better
 *	be fairly small.
 *
 *	In general, GiST needs to search multiple subtrees in order to guarantee
 *	that all occurrences of the same key have been found.  Because of this,
 *	the estimated cost for scanning the index ought to be higher than the
 *	output selectivity would indicate.  gistcostestimate(), over in selfuncs.c,
 *	ought to be adjusted accordingly --- but until we can generate somewhat
 *	realistic numbers here, it hardly matters...
 */


/*
 * Selectivity for operators that depend on area, such as "overlap".
 */

Datum
areasel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

Datum
areajoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

Datum
rangeoverlapsjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid         operator = PG_GETARG_OID(1);
    List       *args = (List *) PG_GETARG_POINTER(2);
    JoinType    jointype = (JoinType) PG_GETARG_INT16(3);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid         collation = PG_GET_COLLATION();

    double      selec = 0.005;

    VariableStatData vardata1;
    VariableStatData vardata2;
	AttStatsSlot histogram1, histogram2;
	AttStatsSlot histogram_bounds1, histogram_bounds2;
	int			nbins1, nbins2; //TODO check if they have to be the same number
	RangeBound 	start1, end1, start2, end2;
	Datum 		hist_start1, hist_start2,
				hist_end1, hist_end2,
				bin_width1, bin_width2;
	int			total_freqs1 = 0;
    int			total_freqs2 = 0;

    Oid         opfuncoid;
    AttStatsSlot sslot1, sslot2;
    Form_pg_statistic stats1 = NULL;
    Form_pg_statistic stats2 = NULL;
    TypeCacheEntry *typcache = NULL;
    bool        join_is_reversed;
    bool        empty;

    get_join_variables(root, args, sjinfo,
                       &vardata1, &vardata2, &join_is_reversed);

    typcache = range_get_typcache(fcinfo, vardata1.vartype);
    opfuncoid = get_opcode(operator);






    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata1, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);

    stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
    /* Try to get fraction of empty ranges */
    get_attstatsslot(&sslot1, vardata1.statsTuple,
                            STATISTIC_KIND_BOUNDS_HISTOGRAM,
                            InvalidOid, ATTSTATSSLOT_VALUES);


                            


    //TODO cleanup this code, maybe creating functions (maybe in common with selfuncs since the code is the same)
	//TODO think about collecting total frequencies as another stastic in typanalyze
    // SNIPPET TO RETRIEVE HISTOGRAM FROM typanalyze
    // first relation
	get_attstatsslot(&histogram1, vardata1.statsTuple,
						   STATISTIC_RANGE_FREQUENCY_HISTOGRAM, InvalidOid,
						   ATTSTATSSLOT_VALUES);
	nbins1 = histogram1.nvalues;
	printf("FIRST HISTOGRAM (%d values)\n", nbins1);
	for (int z=0; z<nbins1; z++) {
		printf("%d; ", histogram1.values[z]);
		total_freqs1 += histogram1.values[z];
	}
	fflush(stdout);

    // second relation
    get_attstatsslot(&histogram2, vardata2.statsTuple,
						   STATISTIC_RANGE_FREQUENCY_HISTOGRAM, InvalidOid,
						   ATTSTATSSLOT_VALUES);
	nbins2 = histogram2.nvalues;
	printf("\nSECOND HISTOGRAM (%d values)\n", nbins2);
	for (int z=0; z<nbins2; z++) {
		printf("%d; ", histogram2.values[z]);
		total_freqs2 += histogram2.values[z];
	}
	fflush(stdout);

	// SNIPPET TO RETRIEVE hg_start AND bin_width FROM typanalyze
    // first relation
	printf("\n\n");
	get_attstatsslot(&histogram_bounds1, vardata1.statsTuple,
						   STATISTIC_RANGE_FREQUENCY_HISTOGRAM_BOUNDS, InvalidOid,
						   ATTSTATSSLOT_VALUES);
	range_deserialize(typcache, DatumGetRangeTypeP(histogram_bounds1.values[0]),
						  &start1, &end1, &empty);
	hist_start1 = start1.val;
	hist_end1 = end1.val;
	bin_width1 = (hist_end1 - hist_start1) / nbins1;
	printf("First bounds: start=%d, end=%d, bin_width=%d\n",
		DatumGetInt32(hist_start1), DatumGetInt32(hist_end1), DatumGetInt32(bin_width1));
	fflush(stdout);
    // second relation
	get_attstatsslot(&histogram_bounds2, vardata2.statsTuple,
						   STATISTIC_RANGE_FREQUENCY_HISTOGRAM_BOUNDS, InvalidOid,
						   ATTSTATSSLOT_VALUES);
	range_deserialize(typcache, DatumGetRangeTypeP(histogram_bounds2.values[0]),
						  &start2, &end2, &empty);
	hist_start2 = start2.val;
	hist_end2 = end2.val;
	bin_width2 = (hist_end2 - hist_start2) / nbins2;
	printf("\nSecond stats: start=%d, end=%d, bin_width=%d\n",
		DatumGetInt32(hist_start2), DatumGetInt32(hist_end2), DatumGetInt32(bin_width2));
	fflush(stdout);


    long count=0;
    int i1 = 0;
    int i2 = 0;
    Datum bin_start1, bin_end1, bin_start2, bin_end2;

    bin_start1 = hist_start1;
    bin_start2 = hist_start2;
    
    while (i1<nbins1 && i2<nbins2) {
        bin_end2 = bin_start2 + bin_width2;
        if (bin_end2<=bin_start1) {
            bin_start2 = bin_end2;
            i2++;
            continue;
        }

        bin_end1 = bin_start1 + bin_width1;
        if (bin_end1 <= bin_start2) {
            bin_start1 = bin_end1;
            i1++;
            continue;
        }

        count += histogram1.values[i1] * histogram2.values[i2];

        if (bin_end1 < bin_end2) {
            // increment in the first histogram
            bin_start1 = bin_end1;
            i1++;
        } else if (bin_end1 == bin_end2) {
            // increment in both the histograms
            bin_start1 = bin_end1;
            i1++;
            bin_start2 = bin_end2;
            i2++;
        } else {
            // increment in the second histogram
            bin_start2 = bin_end2;
            i2++;
        }
    }

    // Normalizing the counter and getting the percentage
    // ("The number of rows that the join is likely to emit is calculated as the cardinality
    // of the Cartesian product of the two inputs multiplied by the selectivity")
    printf("count: %ld, total_freq1: %d, total_freq2 %d\n", count, total_freqs1, total_freqs2);
    float8 selectivity = count / ((double)total_freqs1*total_freqs2); //TODO check if this formula is correct
    printf("Selectivity: %f\n", selectivity);
    
    free_attstatsslot(&histogram1);
    free_attstatsslot(&histogram2);
    free_attstatsslot(&histogram_bounds1);
    free_attstatsslot(&histogram_bounds2);

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    PG_RETURN_FLOAT8(selectivity);
}

/*
 *	positionsel
 *
 * How likely is a box to be strictly left of (right of, above, below)
 * a given box?
 */

Datum
positionsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

Datum
positionjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

/*
 *	contsel -- How likely is a box to contain (be contained by) a given box?
 *
 * This is a tighter constraint than "overlap", so produce a smaller
 * estimate than areasel does.
 */

Datum
contsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}

Datum
contjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}
