#include "postgres.h"

#include "mb/pg_wchar.h"
#include "regex/regex.h"
#include "regex/regexport.h"

typedef struct PgZoektRegexHandle
{
	regex_t		regex;
} PgZoektRegexHandle;

typedef struct PgZoektRegexArc
{
	int			co;
	int			to;
} PgZoektRegexArc;

PgZoektRegexHandle *
pg_zoekt_regex_compile(const char *pattern, int pattern_len, bool case_insensitive,
					   Oid collation, int *errcode)
{
	PgZoektRegexHandle *handle;
	pg_wchar   *wide;
	int			wide_len;
	int			cflags;
	int			result;

	handle = palloc(sizeof(PgZoektRegexHandle));
	wide = palloc(sizeof(pg_wchar) * (pattern_len + 1));
	wide_len = pg_mb2wchar_with_len(pattern, wide, pattern_len);
	cflags = REG_ADVANCED | REG_NOSUB;
	if (case_insensitive)
		cflags |= REG_ICASE;

	result = pg_regcomp(&handle->regex, wide, wide_len, cflags, collation);
	pfree(wide);

	if (result != REG_OKAY)
	{
		if (errcode)
			*errcode = result;
		pfree(handle);
		return NULL;
	}

	if (errcode)
		*errcode = REG_OKAY;
	return handle;
}

void
pg_zoekt_regex_free(PgZoektRegexHandle *handle)
{
	if (handle == NULL)
		return;
	pg_regfree(&handle->regex);
	pfree(handle);
}

int
pg_zoekt_regex_num_states(const PgZoektRegexHandle *handle)
{
	return pg_reg_getnumstates(&handle->regex);
}

int
pg_zoekt_regex_initial_state(const PgZoektRegexHandle *handle)
{
	return pg_reg_getinitialstate(&handle->regex);
}

int
pg_zoekt_regex_final_state(const PgZoektRegexHandle *handle)
{
	return pg_reg_getfinalstate(&handle->regex);
}

int
pg_zoekt_regex_num_out_arcs(const PgZoektRegexHandle *handle, int state)
{
	return pg_reg_getnumoutarcs(&handle->regex, state);
}

void
pg_zoekt_regex_get_out_arcs(const PgZoektRegexHandle *handle, int state,
							PgZoektRegexArc *arcs, int arcs_len)
{
	pg_reg_getoutarcs(&handle->regex, state, (regex_arc_t *) arcs, arcs_len);
}

int
pg_zoekt_regex_num_colors(const PgZoektRegexHandle *handle)
{
	return pg_reg_getnumcolors(&handle->regex);
}

bool
pg_zoekt_regex_color_is_begin(const PgZoektRegexHandle *handle, int color)
{
	return pg_reg_colorisbegin(&handle->regex, color) != 0;
}

bool
pg_zoekt_regex_color_is_end(const PgZoektRegexHandle *handle, int color)
{
	return pg_reg_colorisend(&handle->regex, color) != 0;
}

int
pg_zoekt_regex_num_characters(const PgZoektRegexHandle *handle, int color)
{
	return pg_reg_getnumcharacters(&handle->regex, color);
}

void
pg_zoekt_regex_get_characters(const PgZoektRegexHandle *handle, int color,
							  pg_wchar *chars, int chars_len)
{
	pg_reg_getcharacters(&handle->regex, color, chars, chars_len);
}
