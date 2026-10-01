package com.microsoft.mqtt.test.hivemq;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
import com.hivemq.extension.sdk.api.services.Services;

public final class CustomCounterExtensionMain implements ExtensionMain {
    @Override
    public void extensionStart(ExtensionStartInput input, ExtensionStartOutput output) {
        Services.securityRegistry()
                .setEnhancedAuthenticatorProvider(ignored -> new CustomCounterAuthenticator());
    }

    @Override
    public void extensionStop(ExtensionStopInput input, ExtensionStopOutput output) {}
}