"""APNS Router"""
import socket
import uuid
from typing import Any  # noqa

from hyper.http20.exceptions import ConnectionError, HTTP20Error
from twisted.internet.threads import deferToThread
from twisted.logger import Logger

from autopush.exceptions import RouterException
from autopush.metrics import make_tags
from autopush.router.apns2 import (
    APNSClient,
    APNS_MAX_CONNECTIONS,
)
from autopush.router.interface import RouterResponse
from autopush.types import JSONDict  # noqa


# https://github.com/djacobs/PyAPNs
class APNSRouter(object):
    """APNS Router Implementation"""
    log = Logger()
    apns = None

    def _connect(self, rel_channel, load_connections=True):
        """Connect to APNS

        :param rel_channel: Release channel name (e.g. Firefox. FirefoxBeta,..)
        :type rel_channel: str
        :param load_connections: (used for testing)
        :type load_connections: bool

        :returns: APNs to be stored under the proper release channel name.
        :rtype: apns.APNs

        """
        default_topic = "com.mozilla.org." + rel_channel
        cert_info = self.router_conf[rel_channel]
        return APNSClient(
            cert_file=cert_info.get("cert"),
            key_file=cert_info.get("key"),
            use_sandbox=cert_info.get("sandbox", False),
            max_connections=cert_info.get("max_connections",
                                          APNS_MAX_CONNECTIONS),
            topic=cert_info.get("topic", default_topic),
            logger=self.log,
            metrics=self.metrics,
            load_connections=load_connections,
            max_retry=cert_info.get('max_retry', 2)
        )

    def __init__(self, conf, router_conf, metrics, load_connections=True):
        """Create a new APNS router and connect to APNS

        :param conf: Configuration settings
        :type conf: autopush.config.AutopushConfig
        :param router_conf: Router specific configuration
        :type router_conf: dict
        :param load_connections: (used for testing)
        :type load_connections: bool

        """
        self.conf = conf
        self.router_conf = router_conf
        self.metrics = metrics
        self._base_tags = ["platform:apns"]
        self.apns = dict()
        for rel_channel in router_conf:
            self.apns[rel_channel] = self._connect(rel_channel,
                                                   load_connections)
        self.log.debug("Starting APNS router...")

    def register(self, uaid, router_data, app_id, *args, **kwargs):
        # type: (str, JSONDict, str, *Any, **Any) -> None
        """Register an endpoint for APNS, on the `app_id` release channel.

        This will validate that an APNs instance token is in the
        `router_data`,

        :param uaid: User Agent Identifier
        :param router_data: Dict containing router specific configuration info
        :param app_id: The release channel identifier for cert info lookup

        """
        if app_id not in self.apns:
            raise RouterException("Unknown release channel specified",
                                  status_code=400,
                                  response_body="Unknown release channel")
        if not router_data.get("token"):
            raise RouterException("No token registered", status_code=400,
                                  response_body="No token registered")
        router_data["rel_channel"] = app_id

    def amend_endpoint_response(self, response, router_data):
        # type: (JSONDict, JSONDict) -> None
        """Stubbed out for this router"""

    def route_notification(self, notification, uaid_data):
        """Start the APNS notification routing, returns a deferred

        :param notification: Notification data to send
        :type notification: autopush.endpoint.Notification
        :param uaid_data: User Agent specific data
        :type uaid_data: dict

        """
        router_data = uaid_data["router_data"]
        # Kick the entire notification routing off to a thread
        return deferToThread(self._route, notification, router_data)

    def _route(self, notification, router_data):
        """Blocking APNS call to route the notification

        :param notification: Notification data to send
        :type notification: dict
        :param router_data: Pre-initialized data for this connection
        :type router_data: dict

        """
        router_token = router_data["token"]
        rel_channel = router_data["rel_channel"]
        apns_client = self.apns[rel_channel]
        # chid MUST MATCH THE CHANNELID GENERATED BY THE REGISTRATION SERVICE
        # Currently this value is in hex form.
        payload = {
            "chid": notification.channel_id.hex,
            "ver": notification.version,
        }
        if notification.data:
            payload["body"] = notification.data
            payload["con"] = notification.headers["encoding"]

            if "encryption" in notification.headers:
                payload["enc"] = notification.headers["encryption"]
            if "crypto_key" in notification.headers:
                payload["cryptokey"] = notification.headers["crypto_key"]
            elif "encryption_key" in notification.headers:
                payload["enckey"] = notification.headers["encryption_key"]
            payload['aps'] = router_data.get('aps', {
                "mutable-content": 1,
                "alert": {
                    "loc-key": "SentTab.NoTabArrivingNotification.body",
                    "title-loc-key": "SentTab.NoTabArrivingNotification.title",
                }
            })
        apns_id = str(uuid.uuid4()).lower()
        # APNs may force close a connection on us without warning.
        # if that happens, retry the message.
        try:
            apns_client.send(router_token=router_token, payload=payload,
                             apns_id=apns_id)
        except Exception as e:
            # We sometimes see strange errors around sending push notifications
            # to APNS. We get reports that after a new deployment things work,
            # but then after a week or so, messages across the APNS bridge
            # start to fail. The connections appear to be working correctly,
            # so we don't think that this is a problem related to how we're
            # connecting.
            if isinstance(e, ConnectionError):
                reason = "connection_error"
            elif isinstance(e, (HTTP20Error, socket.error)):
                reason = "http2_error"
            else:
                reason = "unknown"
            self.metrics.increment("notification.bridge.connection.error",
                                   tags=make_tags(self._base_tags,
                                                  application=rel_channel,
                                                  reason=reason))
            raise RouterException(
                str(e),
                status_code=502,
                response_body="APNS returned an error processing request",
            )

        location = "%s/m/%s" % (self.conf.endpoint_url, notification.version)
        self.metrics.increment("notification.bridge.sent",
                               tags=make_tags(self._base_tags,
                                              application=rel_channel))

        self.metrics.increment(
            "updates.client.bridge.apns.{}.sent".format(
                router_data["rel_channel"]
            ),
            tags=self._base_tags
        )
        self.metrics.increment("notification.message_data",
                               notification.data_length,
                               tags=make_tags(self._base_tags,
                                              destination='Direct'))
        return RouterResponse(status_code=201, response_body="",
                              headers={"TTL": notification.ttl,
                                       "Location": location},
                              logged_status=200)
