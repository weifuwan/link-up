package org.apache.cockpit.connectors.api.factory;


import org.apache.cockpit.connectors.api.config.OptionRule;

/** todo: use PluginIdentifier. This is the SPI interface. */
public interface Factory {

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    String factoryIdentifier();

    /**
     * Returns the rule for options.
     *
     * <p>1. Used to verify whether the parameters configured by the user conform to the rules of
     * the options;
     *
     * <p>2. Used for Web-UI to prompt user to configure option value;
     */
    OptionRule optionRule();
}
