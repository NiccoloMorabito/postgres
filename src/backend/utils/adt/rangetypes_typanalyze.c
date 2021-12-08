/*-------------------------------------------------------------------------
 *
 * rangetypes_typanalyze.c
 *	  Functions for gathering statistics from range columns
 *
 * For a range type column, histograms of lower and upper bounds, and
 * the fraction of NULL and empty ranges are collected.
 *
 * Both histograms have the same length, and they are combined into a
 * single array of ranges. This has the same shape as the histogram that
 * std_typanalyze would collect, but the values are different. Each range
 * in the array is a valid range, even though the lower and upper bounds
 * come from different tuples. In theory, the standard scalar selectivity
 * functions could be used with the combined histogram.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/rangetypes_typanalyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_operator.h"
#include "commands/vacuum.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rangetypes.h"

static int	float8_qsort_cmp(const void *a1, const void *a2);
static int	range_bound_qsort_cmp(const void *a1, const void *a2, void *arg);
static void compute_range_stats(VacAttrStats *stats,
								AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows);

/*
 * range_typanalyze -- typanalyze function for range columns
 */
Datum
range_typanalyze(PG_FUNCTION_ARGS)
{
	VacAttrStats *stats = (VacAttrStats *) PG_GETARG_POINTER(0);
	TypeCacheEntry *typcache;
	Form_pg_attribute attr = stats->attr;

	/* Get information about range type; note column might be a domain */
	typcache = range_get_typcache(fcinfo, getBaseType(stats->attrtypid));

	if (attr->attstattarget < 0)
		attr->attstattarget = default_statistics_target;

	stats->compute_stats = compute_range_stats;
	stats->extra_data = typcache;
	/* same as in std_typanalyze */
	stats->minrows = 300 * attr->attstattarget;

	PG_RETURN_BOOL(true);
}

/*
 * Comparison function for sorting float8s, used for range lengths.
 */
static int
float8_qsort_cmp(const void *a1, const void *a2)
{
	const float8 *f1 = (const float8 *) a1;
	const float8 *f2 = (const float8 *) a2;

	if (*f1 < *f2)
		return -1;
	else if (*f1 == *f2)
		return 0;
	else
		return 1;
}

/*
 * Comparison function for sorting RangeBounds.
 */
static int
range_bound_qsort_cmp(const void *a1, const void *a2, void *arg)
{
	RangeBound *b1 = (RangeBound *) a1;
	RangeBound *b2 = (RangeBound *) a2;
	TypeCacheEntry *typcache = (TypeCacheEntry *) arg;

	return range_cmp_bounds(typcache, b1, b2);
}

/*
 * compute_range_stats() -- compute statistics for a range column
 */
static void
compute_range_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
					int samplerows, double totalrows)
{
	printf("Beginning compute_range_stats...\n");
	fflush(stdout);
	TypeCacheEntry *typcache = (TypeCacheEntry *) stats->extra_data;
	bool		has_subdiff = OidIsValid(typcache->rng_subdiff_finfo.fn_oid);
	int			null_cnt = 0;
	int			non_null_cnt = 0;
	int			non_empty_cnt = 0;
	int			empty_cnt = 0;
	int			range_no;
	int			slot_idx;
	int			num_bins = stats->attr->attstattarget;
	int			num_hist;
	RangeBound *lowers,
			   *uppers;
	double		total_width = 0;

	// Allocate memory to hold range bounds and lengths of the sample ranges
	lowers = (RangeBound *) palloc(sizeof(RangeBound) * samplerows);
	uppers = (RangeBound *) palloc(sizeof(RangeBound) * samplerows);

	// Loop over the sample ranges
	for (range_no = 0; range_no < samplerows; range_no++)
	{
		Datum		value;
		bool		isnull,
					empty;
		RangeType  *range;
		RangeBound	lower,
					upper;

		vacuum_delay_point();

		value = fetchfunc(stats, range_no, &isnull);
		if (isnull)
		{
			// range is null, just count that
			null_cnt++;
			continue;
		}

		/*
		 * XXX: should we ignore wide values, like std_typanalyze does, to
		 * avoid bloating the statistics table?
		 */
		total_width += VARSIZE_ANY(DatumGetPointer(value));

		// Get range and deserialize it for further analysis
		range = DatumGetRangeTypeP(value);
		range_deserialize(typcache, range, &lower, &upper, &empty);

		if (!empty)
		{
			//Remember bounds and length for further usage in histograms
			lowers[non_empty_cnt] = lower;
			uppers[non_empty_cnt] = upper;

			non_empty_cnt++;
		}
		else
			empty_cnt++;

		non_null_cnt++;
	}

	slot_idx = 0;

	// We can only compute real stats if we found some non-null values
	if (non_null_cnt > 0)
	{
		int32	   *histogram;
		RangeBound	min_bound, max_bound;
		Datum		start_hist,
					end_hist,
					bin_width,
					bin_start,
					bin_end,
					lower_bound,
					upper_bound;
		MemoryContext old_cxt;
		float4	   *emptyfrac;
		int 		i, frequency;

		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */ //TODO are these needed?
		stats->stanullfrac = (double) null_cnt / (double) samplerows;
		stats->stawidth = total_width / (double) non_null_cnt;

		/* Estimate that non-null values are unique */
		stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);

		/* Must copy the target values into anl_context */
		old_cxt = MemoryContextSwitchTo(stats->anl_context);

		/*
		 * Generate a frequency histogram if there are at least two values.
		 */
		if (non_empty_cnt >= 2)
		{
			/* Sort bound values */
			qsort_arg(lowers, non_empty_cnt, sizeof(RangeBound),
					  range_bound_qsort_cmp, typcache);
			qsort_arg(uppers, non_empty_cnt, sizeof(RangeBound),
					  range_bound_qsort_cmp, typcache);

			min_bound = lowers[0];
			max_bound = uppers[non_empty_cnt-1];
			// start_hist = min(lower_bounds)
			start_hist = min_bound.val;
			// end_hist = max(upper_bounds)
			end_hist = max_bound.val;							

			//TODO check if this approach is ok 
			num_hist = non_empty_cnt;
			if (num_hist > num_bins)
				num_hist = num_bins + 1;

			bin_width = (end_hist - start_hist) / num_hist; //TODO NEED TO USE SUBDIFF
			//TODO this rounds down the result, ^^^^^^losing a lot of values potentially
			printf("start_hg: %d\n", DatumGetInt32(start_hist));
			printf("end_hg: %d\n", DatumGetInt32(end_hist));
			printf("bin_width: %d\n", DatumGetInt32(bin_width));
			fflush(stdout);
			
			histogram = (int32 *) palloc(num_hist * sizeof(int32));
			printf("Printing frequencies...\n");
			for (i = 0; i < num_hist; i++)
			{
				bin_start = start_hist + bin_width*i;
				bin_end = bin_start + bin_width;
				frequency = 0;
				
				for (int r=0; r<non_empty_cnt; r++)
				{
					lower_bound = lowers[r].val;
					upper_bound = uppers[r].val;
					// if overlaps
					if (lower_bound < bin_end && upper_bound > bin_start)
					{
						frequency++;
					}
				}
				printf("%d; ", frequency);
				
				histogram[i] = frequency;
			}
			printf("\nPrinting histogram\n");
			for (int z=0; z<num_hist; z++) {
				printf("%d; ", histogram[z]);
			}
			printf("\n\n");

			// FREQUENCY HISTOGRAM
			stats->staop[slot_idx] = Int4LessOperator;
			stats->stacoll[slot_idx] = InvalidOid;
			stats->stavalues[slot_idx] = histogram;
			stats->numvalues[slot_idx] = num_hist;
			stats->statypid[slot_idx] = INT4OID;
			stats->statyplen[slot_idx] = sizeof(int64);
			stats->statypbyval[slot_idx] = true;
			stats->statypalign[slot_idx] = 'd';

			//Store the fraction of empty ranges //TODO check if these values are needed
			emptyfrac = (float4 *) palloc(sizeof(float4));
			*emptyfrac = ((double) empty_cnt) / ((double) non_null_cnt);
			stats->stanumbers[slot_idx] = emptyfrac;
			stats->numnumbers[slot_idx] = 1;

			stats->stakind[slot_idx] = STATISTIC_RANGE_FREQUENCY_HISTOGRAM;
			slot_idx++;

			// HISTOGRAM FOR START AND END OF FREQUENCY HISTOGRAM
			Datum * histogramBounds = (Datum *) palloc(sizeof(Datum));
			histogramBounds[0] = PointerGetDatum(
				range_serialize(typcache, &min_bound, &max_bound, false));
			stats->stakind[slot_idx] = STATISTIC_RANGE_FREQUENCY_HISTOGRAM_BOUNDS;
			stats->stavalues[slot_idx] = histogramBounds;
			stats->numvalues[slot_idx] = 1;
		}

		MemoryContextSwitchTo(old_cxt);
	}
	else if (null_cnt > 0)
	{
		// We found only nulls; assume the column is entirely null
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;
		stats->stawidth = 0;
		stats->stadistinct = 0.0;
	}

	/*
	 * We don't need to bother cleaning up any of our temporary palloc's. The
	 * hashtable should also go away, as it used a child memory context.
	 */
	printf("End of function\n");
	fflush(stdout);
}
