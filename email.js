'use strict';

var elasticsearch = require('elasticsearch');
var raven = require('raven');
var Q = require('q');
var rp = require('request-promise');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var elasticSearchClient = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Initialize Google Cloud
var topicName = 'process-email-change-bulk';
var subscriptionName = 'node-email-change-bulk';
var pubsub = gcloud.pubsub();

var emailIdTopicName = 'process-email-change';

// Instantiate a sentry client
var sentryClient = new raven.Client('https://0366ffd1a51a4fc4881b7e7bfca378d6:6191273b778d4033a7f16d8c0f020366@sentry.io/137174');
sentryClient.patchGlobal();

// Get a Google Cloud topic
function getTopic(currentTopicName, cb) {
    pubsub.createTopic(currentTopicName, function(err, topic) {
        // topic already exists.
        if (err && err.code === 409) {
            return cb(null, pubsub.topic(currentTopicName));
        }
        return cb(err, topic);
    });
}

function addEmailIdToPubSubTopicPublish(emailIds) {
    var deferred = Q.defer();

    getTopic(emailIdTopicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    Id: emailIds,
                    Method: 'create'
                }
            }, function(err) {
                if (err) {
                    deferred.reject(new Error(err));
                    console.error('Error occurred while queuing background task', err);
                    sentryClient.captureMessage(err);
                } else {
                    deferred.resolve(true);
                }
            });
        }
    });

    return deferred.promise;
}

function addEmailIdToPubSub(emailIds) {
    var allPromises = [];
    var emailIdsArray = emailIds.split(",");

    var i, j, tempArray, chunk = 75;
    for (i = 0, j = emailIdsArray.length; i < j; i += chunk) {
        // Break array into a chunk
        tempArray = emailIdsArray.slice(i, i + chunk);
        var tempString = tempArray.join(',');

        // Execute contact sync
        var toExecute = addEmailIdToPubSubTopicPublish(tempString);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function processEmails(data) {
    var deferred = Q.defer();

    addEmailIdToPubSub(data.EmailId).then(function(contactStatus) {
        deferred.resolve(contactStatus);
    }, function(error) {
        console.error(error);
        sentryClient.captureMessage(error);
    });

    return deferred.promise;
}

// Subscribe to Pub/Sub for this particular topic
function subscribe(cb) {
    var subscription;

    // Event handlers
    function handleMessage(message) {
        cb(null, message);
    }

    function handleError(err) {
        sentryClient.captureMessage(err);
        console.error(err);
    }

    getTopic(topicName, function(err, topic) {
        if (err) {
            return cb(err);
        }

        topic.subscribe(subscriptionName, {
            autoAck: true,
            reuseExisting: true
        }, function(err, sub) {
            if (err) {
                return cb(err);
            }

            subscription = sub;

            // Listen to and handle message and error events
            subscription.on('message', handleMessage);
            subscription.on('error', handleError);

            console.log('Listening to ' + topicName +
                ' with subscription ' + subscriptionName);
        });
    });

    // Subscription cancellation function
    return function() {
        if (subscription) {
            // Remove event listeners
            subscription.removeListener('message', handleMessage);
            subscription.removeListener('error', handleError);
            subscription = undefined;
        }
    };
}

// Begin subscription
subscribe(function(err, message) {
    // Any errors received are considered fatal.
    if (err) {
        sentryClient.captureMessage(err);
        console.error(err);
        throw err;
    }
    console.log('Received request to process emailid bulk ' + message.data.EmailId);
    processEmails(message.data)
        .then(function(status) {
            rp('https://hchk.io/574240c8-248a-4f9a-9a70-aacad6748636')
                .then(function(htmlString) {
                    console.log('Completed execution for ' + message.data.EmailId);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        });
});
