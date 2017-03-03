'use strict';

var raven = require('raven');
var rp = require('request-promise');
var elasticsearch = require('elasticsearch');
var Q = require('q');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a datastore client
var datastore = require('@google-cloud/datastore')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://c2b3c727812f4643b73f40bee09e5108:fed6658dfeb94757b53cb062e81cdc68@sentry.io/103136');
sentryClient.patchGlobal();

// Initialize Google Cloud
var topicName = 'process-email-change';
var subscriptionName = 'node-email-change';
var pubsub = gcloud.pubsub();

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

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {Object} requestData Cloud Function request data.
 * @param {string} requestData.Id Datastore ID string.
 * @returns {Object} Datastore key object.
 */
function getKeyFromRequestData(requestData) {
    if (!requestData.Id) {
        throw new Error('Id not provided. Make sure you have a "Id" property ' +
            'in your request');
    }

    var ids = requestData.Id.split(',');
    var keys = [];

    for (var i = ids.length - 1; i >= 0; i--) {
        var emailId = parseInt(ids[i], 10);
        var datastoreId = datastore.key(['Email', emailId]);
        keys.push(datastoreId);
    }

    return keys;
}

/**
 * Retrieves a record.
 *
 * @example
 * gcloud alpha functions call ds-get --data '{"kind":"gcf-test","key":"foobar"}'
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the user.
 * @param {string} data.kind The Datastore kind of the data to retrieve, e.g. "user".
 * @param {string} data.key Key at which to retrieve the data, e.g. 5075192766267392.
 */
function getDatastore(data) {
    var deferred = Q.defer();
    try {
        var keys = getKeysFromRequestData(data, resouceType);

        datastore.get(keys, function(err, entities) {
            if (err) {
                console.error(err);
                sentryClient.captureMessage(err);
                deferred.reject(new Error(err));
            }

            // The get operation will not fail for a non-existent entities, it just
            // returns null.
            if (!entities) {
                var error = 'Entity does not exist';
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
            }

            deferred.resolve(entities);
        });

    } catch (err) {
        console.error(err);
        sentryClient.captureMessage(err);
        deferred.reject(new Error(err));
    }

    return deferred.promise;
}

/**
 * Format a user for ES sync
 *
 * @param {Object} emailData User details from datastore.
 */
function formatESEmail(emailId, emailData) {
    emailData['Id'] = emailId;
    return emailData;
}

/**
 * Add a user to ES
 *
 * @param {Object} userData User details from datastore.
 * Returns true if adding data works and false if not.
 */
function addToElastic(emails) {
    var deferred = Q.defer();
    var esActions = [];

    for (var i = emails.length - 1; i >= 0; i--) {
        var emailData = emails[i].data;
        var emailId = emails[i].key.id;
        var postEmailData = formatESEmail(emailId, emailData);

        var indexRecord = {
            index: {
                _index: 'emails',
                _type: 'email',
                _id: emailId
            }
        };
        var dataRecord = postEmailData;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    client.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            sentryClient.captureMessage(error);
            deferred.reject(false);
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

/**
 * Syncs a email information to elasticsearch.
 *
 * @param {Object} email Email details from datastore.
 */
function getAndSyncElastic(emails) {
    var deferred = Q.defer();

    addToElastic(emails).then(function(status) {
        if (status) {
            deferred.resolve(true);
        } else {
            deferred.resolve(false);
        }
    });

    return deferred.promise;
}

function syncEmails(data) {
    var deferred = Q.defer();

    getDatastore(data).then(function(emails) {
        if (emails != null) {
            getAndSyncElastic(emails).then(function(elasticResponse) {
                if (elasticResponse) {
                    deferred.resolve(true);
                } else {
                    var error = 'Elastic sync failed';
                    sentryClient.captureMessage(error);
                    deferred.reject(new Error(error));
                    throw new Error(error);
                }
            });
        } else {
            var error = 'Elastic sync failed';
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
            throw new Error(error);
        }
    }, function(error) {
        sentryClient.captureMessage(error);
        deferred.reject(new Error(error));
        throw new Error(error);
    });

    return deferred.promise;
};

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
    console.log('Received request to process email ' + message.data.Id);
    syncEmails(message.data)
        .then(function(status) {
            rp('https://hchk.io/998c5b7a-54fe-4e28-8f68-32cb0775ab46')
                .then(function(htmlString) {
                    console.log('Completed execution for ' + message.data.Id);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        });
});

// testSync({Id: '5036688686448640'});