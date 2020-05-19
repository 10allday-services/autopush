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

    def _config(self, rel_channel, key, default=None):
        try:
            return self.router_conf[rel_channel][key]
        except KeyError:
            try:
                return self.router_conf["_global"][key]
            except KeyError:
                return default

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
        return APNSClient(
            cert_file=self._config(rel_channel, "cert"),
            key_file=self._config(rel_channel, "key"),
            use_sandbox=self._config(rel_channel, "sandbox", False),
            max_connections=self._config(
                rel_channel, "max_connections", APNS_MAX_CONNECTIONS),
            topic=self._config(rel_channel, "topic", default_topic),
            logger=self.log,
            metrics=self.metrics,
            load_connections=load_connections,
            max_retry=self._config(rel_channel, 'max_retry', 2),
            conn_ttl=self._config(rel_channel, 'connection_ttl', 30),
            reap_sleep=self._config(
                rel_channel, 'connection_reap_cycle', 60),
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
        success = False
        try:
            apns_client.send(router_token=router_token, payload=payload,
                             apns_id=apns_id)
            success = True
        except RouterException:
            # Not sure if this is happening, but
            raise
        except ConnectionError:
            self.metrics.increment("notification.bridge.connection.error",
                                   tags=make_tags(
                                       self._base_tags,
                                       application=rel_channel,
                                       reason="connection_error"))
        except (HTTP20Error, IOError, socket.error):
            self.metrics.increment("notification.bridge.connection.error",
                                   tags=make_tags(self._base_tags,
                                                  application=rel_channel,
                                                  reason="http2_error"))
        if not success:
            raise RouterException(
                "Server error",
                status_code=502,
                response_body="APNS returned an error processing request",
                log_exception=False,
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
