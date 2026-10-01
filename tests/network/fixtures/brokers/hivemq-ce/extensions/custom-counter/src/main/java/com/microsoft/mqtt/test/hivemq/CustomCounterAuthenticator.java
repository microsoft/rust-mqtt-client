package com.microsoft.mqtt.test.hivemq;

import com.hivemq.extension.sdk.api.auth.EnhancedAuthenticator;
import com.hivemq.extension.sdk.api.auth.parameter.EnhancedAuthConnectInput;
import com.hivemq.extension.sdk.api.auth.parameter.EnhancedAuthInput;
import com.hivemq.extension.sdk.api.auth.parameter.EnhancedAuthOutput;
import java.nio.ByteBuffer;
import java.util.Optional;

public final class CustomCounterAuthenticator implements EnhancedAuthenticator {
    private static final String METHOD = "CUSTOM-COUNTER-METHOD";

    private boolean awaitingFinalResponse;

    @Override
    public void onConnect(EnhancedAuthConnectInput input, EnhancedAuthOutput output) {
        var packet = input.getConnectPacket();
        var method = packet.getAuthenticationMethod();
        if (method.isEmpty() || !METHOD.equals(method.get())) {
            output.nextExtensionOrDefault();
            return;
        }
        beginExchange(packet.getAuthenticationData(), output);
    }

    @Override
    public void onReAuth(EnhancedAuthInput input, EnhancedAuthOutput output) {
        var packet = input.getAuthPacket();
        if (!METHOD.equals(packet.getAuthenticationMethod())) {
            output.nextExtensionOrDefault();
            return;
        }
        beginExchange(packet.getAuthenticationData(), output);
    }

    @Override
    public void onAuth(EnhancedAuthInput input, EnhancedAuthOutput output) {
        var packet = input.getAuthPacket();
        if (!METHOD.equals(packet.getAuthenticationMethod())) {
            output.nextExtensionOrDefault();
            return;
        }
        if (!awaitingFinalResponse || !dataIs(packet.getAuthenticationData(), (byte) '3')) {
            output.failAuthentication();
            return;
        }

        awaitingFinalResponse = false;
        output.authenticateSuccessfully();
    }

    private void beginExchange(Optional<ByteBuffer> data, EnhancedAuthOutput output) {
        if (!dataIs(data, (byte) '1')) {
            output.failAuthentication();
            return;
        }

        awaitingFinalResponse = true;
        output.continueAuthentication(new byte[] {'2'});
    }

    private static boolean dataIs(Optional<ByteBuffer> data, byte expected) {
        return data.filter(buffer ->
                        buffer.remaining() == 1 && buffer.get(buffer.position()) == expected)
                .isPresent();
    }
}