package org.apache.cockpit.connectors.api.common.metrics;

import java.util.function.Predicate;
import java.util.regex.Pattern;

/** Static utility class for creating various {@link Measurement} filtering predicates. */
public final class MeasurementPredicates {

    private MeasurementPredicates() {}

    /**
     * Matches a {@link Measurement} which contain the specified tag.
     *
     * @param tag the tag of interest
     * @return a filtering predicate
     */
    public static Predicate<Measurement> containsTag(String tag) {
        return measurement -> measurement.tag(tag) != null;
    }

    /**
     * Matches a {@link Measurement} which contains the specified tag and the tag has the specified
     * value.
     *
     * @param tag the tag to match
     * @param value the value the tag has to have
     * @return a filtering predicate
     */
    public static Predicate<Measurement> tagValueEquals(String tag, String value) {
        return measurement -> value.equals(measurement.tag(tag));
    }

    /**
     * Matches a {@link Measurement} which has this exact tag with a value matching the provided
     * regular expression.
     *
     * @param tag the tag to match
     * @param valueRegexp regular expression to match the value against
     * @return a filtering predicate
     */
    public static Predicate<Measurement> tagValueMatches(String tag, String valueRegexp) {
        return measurement -> {
            String value = measurement.tag(tag);
            return value != null && Pattern.compile(valueRegexp).matcher(value).matches();
        };
    }
}
