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

#ifdef __cplusplus
}
#endif
