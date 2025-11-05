// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License

//! Portable triggering of reauthentication flows

use bytes::Bytes;

use crate::buffer_pool::SharedImpl;
use crate::client::ClientError;
use crate::client::token::completion::ReauthCompletionToken;
use crate::packet::AuthProperties;

pub struct ReauthToken(pub(crate) buffered::ReauthToken<SharedImpl>);

impl ReauthToken {
    pub async fn continue_reauth(
        self,
        authentication_data: Option<Bytes>,
        properties: AuthProperties,
    ) -> Result<ReauthCompletionToken, ClientError> {
        let token = self
            .0
            .continue_reauth(
                authentication_data.as_deref().map(Into::into),
                properties.reason_string.as_deref().map(Into::into),
                crate::packet::map_user_properties_to_bytestr(properties.user_properties),
            )
            .await?;
        Ok(ReauthCompletionToken(token.0))
    }
}

pub(crate) mod buffered {
    use crate::buffer_pool::Shared;
    use crate::client::ClientError;
    use crate::client::channel_data::ReauthRequest;
    use crate::client::token::completion::buffered::{ReauthCompletionToken, completion_pair};
    use crate::mqtt_proto::{
        Auth, AuthenticateReasonCode, Authentication, BinaryData, ByteStr, UserProperties,
    };

    #[derive(Debug)]
    pub struct ReauthToken<S>
    where
        S: Shared,
    {
        pub method: ByteStr<S>,
        pub tx: tokio::sync::mpsc::Sender<ReauthRequest<S>>,
    }

    impl<S> ReauthToken<S>
    where
        S: Shared,
    {
        pub async fn continue_reauth(
            self,
            authentication_data: Option<BinaryData<S>>,
            reason_string: Option<ByteStr<S>>,
            user_properties: UserProperties<S>,
        ) -> Result<ReauthCompletionToken<S>, ClientError> {
            let (notifier, token) = completion_pair();
            let auth = Auth {
                reason_code: AuthenticateReasonCode::ContinueAuthentication,
                authentication: Some(Authentication {
                    method: self.method,
                    data: authentication_data,
                }),
                reason_string,
                user_properties,
            };
            self.tx
                .send(ReauthRequest(notifier, auth))
                .await
                .map_err(|_| ClientError::DetachedClient)?;
            Ok(ReauthCompletionToken(token))
        }
    }

    #[derive(Debug)]
    pub enum ReauthResponse<S>
    where
        S: Shared,
    {
        Continue(Auth<S>, ReauthToken<S>),
        Success(Auth<S>),
        Failure, // Cannot provide Disconnect packet here because it is not guaranteed to be sent by server
    }
}
