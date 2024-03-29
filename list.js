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
var topicName = 'process-new-list-upload';
var subscriptionName = 'node-new-list-upload';
var pubsub = gcloud.pubsub();

var contactIdTopicName = 'process-new-contact-upload';
var publicationIdTopicName = 'process-new-publication-upload';

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

function addContactIdToPubSubTopicPublish(contactIds) {
    var deferred = Q.defer();

    getTopic(contactIdTopicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    Id: contactIds,
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

function addPublicationIdToPubSubTopicPublish(publicationIds) {
    var deferred = Q.defer();

    getTopic(publicationIdTopicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    Id: publicationIds,
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

function addContactIdToPubSub(contactIds) {
    var allPromises = [];
    var contactIdsArray = contactIds.split(",");
    var filteredContactIds = [];

    for (var i = 0; i < contactIdsArray.length; i++) {
        if (contactIdsArray[i] !== 0) {
            filteredContactIds.push(contactIdsArray[i]);
        }
    }

    var i, j, tempArray, chunk = 75;
    for (i = 0, j = filteredContactIds.length; i < j; i += chunk) {
        // Break array into a chunk
        tempArray = filteredContactIds.slice(i, i + chunk);
        var tempString = tempArray.join(',');

        // Execute contact sync
        var toExecute = addContactIdToPubSubTopicPublish(tempString);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function addPublicationIdToPubSub(publicationIds) {
    var allPromises = [];
    var publicationIdsArray = publicationIds.split(",");
    var filteredPublicationIds = [];

    for (var i = 0; i < publicationIdsArray.length; i++) {
        if (publicationIdsArray[i] !== 0) {
            filteredPublicationIds.push(publicationIdsArray[i]);
        }
    }

    var i, j, tempArray, chunk = 75;
    for (i = 0, j = filteredPublicationIds.length; i < j; i += chunk) {
        // Break array into a chunk
        tempArray = filteredPublicationIds.slice(i, i + chunk);
        var tempString = tempArray.join(',');

        // Execute contact sync
        var toExecute = addPublicationIdToPubSubTopicPublish(tempString);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

// Process a particular Twitter user
function processListUpload(data) {
    var deferred = Q.defer();

    addContactIdToPubSub(data.ContactId).then(function(contactStatus) {
        addPublicationIdToPubSub(data.PublicationId).then(function(publicationStatus) {
            deferred.resolve(publicationStatus);
        }, function(error) {
            console.error(error);
            sentryClient.captureMessage(error);
        });
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
    console.log('Received request to process list upload ' + message.data.ListId);
    processListUpload(message.data)
        .then(function(status) {
            rp('https://hchk.io/d01a4dde-670b-4083-b749-e1bc1079d616')
                .then(function(htmlString) {
                    console.log('Completed execution for ' + message.data.ListId);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        });
});