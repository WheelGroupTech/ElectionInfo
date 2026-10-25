/**
 * @file filter.h
 * @brief Non-destructive ProcMon-style filter rules for the voter grid.
 */
#pragma once

#include "voter_table.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define EE_FILTER_VALUE_CCH    256
#define EE_FILTER_MAX_DISTINCT 8000

    typedef enum EeFilterRelation
    {
        EeRel_Is = 0,
        EeRel_IsNot,
        EeRel_LessThan,
        EeRel_MoreThan,
        EeRel_BeginsWith,
        EeRel_EndsWith,
        EeRel_Contains,
        EeRel_Excludes
    } EeFilterRelation;

    typedef enum EeFilterAction
    {
        EeFilt_Include = 0,
        EeFilt_Exclude
    } EeFilterAction;

    typedef struct EeFilterRule
    {
        uint32_t column;
        EeFilterRelation relation;
        EeFilterAction action;
        BOOL enabled;
        wchar_t value[EE_FILTER_VALUE_CCH];
    } EeFilterRule;

    typedef struct EeFilterSet
    {
        EeFilterRule *rules;
        uint32_t count;
        uint32_t cap;
    } EeFilterSet;

    /** @brief Initialize an empty filter set. */
    void EeFilter_Init(EeFilterSet *set);

    /** @brief Free rules and reset the set. */
    void EeFilter_Clear(EeFilterSet *set);

    /**
     * @brief Append a rule. Grows the backing array as needed.
     * @return FALSE on invalid arguments or out of memory.
     */
    BOOL EeFilter_Add(EeFilterSet *set, const EeFilterRule *rule);

    /**
     * @brief Replace the rule at @p index.
     * @return FALSE if @p index is out of range.
     */
    BOOL EeFilter_Set(EeFilterSet *set, uint32_t index, const EeFilterRule *rule);

    /**
     * @brief Remove the rule at @p index, shifting later rules down.
     * @return FALSE if @p index is out of range.
     */
    BOOL EeFilter_Remove(EeFilterSet *set, uint32_t index);

    /**
     * @brief Copy every rule from @p src into @p dst (clears @p dst first).
     * @return FALSE on invalid arguments or out of memory.
     */
    BOOL EeFilter_Copy(EeFilterSet *dst, const EeFilterSet *src);

    /**
     * @brief TRUE if any rule is enabled.
     */
    BOOL EeFilter_HasEnabled(const EeFilterSet *set);

    /**
     * @brief TRUE if the rule can be added. Date columns with less than / more
     *        than require a date compare value.
     */
    BOOL EeFilter_RuleIsValid(const EeFilterRule *rule, const EeVoterTable *table);

    /**
 * @brief ProcMon visibility: same-column includes OR, different columns AND;
 *        any matching exclude hides the row.
 */
    BOOL EeFilter_AcceptsViewRow(const EeFilterSet *set,
                                 const EeVoterTable *table,
                                 uint32_t view_row);

    /**
 * @brief Build display→view map of accepted rows in current sort order.
 *        If no enabled rules, *out_map is NULL and *out_count is row_count.
 */
    BOOL EeFilter_BuildMap(const EeFilterSet *set,
                           const EeVoterTable *table,
                           uint32_t **out_map,
                           uint32_t *out_count);

    /**
 * @brief Distinct cell values for a display column (sorted, case-insensitive).
 *        Stops after @p max_values unique entries. Caller frees *out_values
 *        and each string.
 */
    BOOL EeFilter_CollectDistinct(const EeVoterTable *table,
                                  uint32_t column,
                                  uint32_t max_values,
                                  wchar_t ***out_values,
                                  uint32_t *out_count);

    /** @brief Display label for a relation (never NULL). */
    const wchar_t *EeFilter_RelationText(EeFilterRelation rel);

    /** @brief Display label for Include/Exclude (never NULL). */
    const wchar_t *EeFilter_ActionText(EeFilterAction action);

    /** A column a filter window offers: its title and the rule column index it stands for.
     *  A saved filter names columns by title (and 1-based position in this list), so it
     *  loads into another file whose columns are in a different order. */
    typedef struct EeFilterColumnRef
    {
        const wchar_t *title;
        uint32_t column;
    } EeFilterColumnRef;

    /** Relations a filter window supports, as a bit mask (1u << EeFilterRelation). */
#define EE_FILTER_REL_ALL      0xFFu
#define EE_FILTER_REL_EQUALITY ((1u << EeRel_Is) | (1u << EeRel_IsNot)) /* CVR: is / is not */

    /**
     * @brief Save @p set as a UTF-8 delimited file (with a BOM, for Excel): a header row
     *        "Column, Column Number, Relation, Value, Action, Enabled" and one row per rule.
     * @param path   Destination file (created or replaced).
     * @param set    Rules to save (all of them, enabled or not).
     * @param cols   The window's columns; a rule whose column is not listed is saved
     *               with an empty title.
     * @param ncols  Number of entries in @p cols.
     * @param delim  L',' (CSV) or L'\t' (tab-delimited).
     * @param error_message Optional; receives a message on failure.
     * @param error_cch     Capacity of @p error_message.
     * @return FALSE on invalid arguments or a write error.
     */
    BOOL EeFilter_SaveFile(const wchar_t *path,
                           const EeFilterSet *set,
                           const EeFilterColumnRef *cols,
                           uint32_t ncols,
                           wchar_t delim,
                           wchar_t *error_message,
                           size_t error_cch);

    /**
     * @brief Load rules saved by EeFilter_SaveFile (comma- or tab-delimited, detected from
     *        the header) into @p out, replacing its rules.
     *
     * Columns are matched by title (case-insensitive); when several of @p cols share the
     * title, the one at the saved Column Number wins, else the first. Rules naming a
     * column the window does not have, or a relation outside @p relation_mask, are
     * skipped and described in @p skipped (one per line). "Column Number" and "Enabled"
     * are optional (a rule without Enabled is enabled).
     * @param path          File to read.
     * @param cols          The window's columns.
     * @param ncols         Number of entries in @p cols.
     * @param relation_mask Allowed relations (EE_FILTER_REL_ALL, EE_FILTER_REL_EQUALITY).
     * @param out           Receives the loaded rules (cleared first).
     * @param out_skipped   Optional; receives the number of rules skipped.
     * @param skipped       Optional; receives the skipped rules' descriptions.
     * @param skipped_cch   Capacity of @p skipped.
     * @param error_message Optional; receives a message on failure.
     * @param error_cch     Capacity of @p error_message.
     * @return FALSE when the file cannot be read or is not a saved filter (@p out is then
     *         left empty); TRUE otherwise, even when every rule was skipped.
     */
    BOOL EeFilter_LoadFile(const wchar_t *path,
                           const EeFilterColumnRef *cols,
                           uint32_t ncols,
                           uint32_t relation_mask,
                           EeFilterSet *out,
                           uint32_t *out_skipped,
                           wchar_t *skipped,
                           size_t skipped_cch,
                           wchar_t *error_message,
                           size_t error_cch);

#ifdef __cplusplus
}
#endif
