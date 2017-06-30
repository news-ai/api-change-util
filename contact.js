'use strict';

var raven = require('raven');
var elasticsearch = require('elasticsearch');
var Q = require('q');
var rp = require('request-promise');
var gcloud = require('google-cloud')({
    projectId: 'newsai-1166'
});

// Instantiate a datastore client
var datastore = require('@google-cloud/datastore')({
    projectId: 'newsai-1166'
});

// Initialize Google Cloud
var enhanceTopicName = 'process-enhance';
var newTwitterTopicName = 'process-twitter-feed'
var newInstagramTopicName = 'process-instagram-feed';
var topicName = 'process-new-contact-upload';
var subscriptionName = 'node-new-contact-upload';
var pubsub = gcloud.pubsub();

// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://f4ab035568994293b9a2a90727ccb5fc:a6dc87433f284952b2b5d629422ef7e6@sentry.io/103134');
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

function addContactEmailsToPubSubTopicPublish(contactEmails) {
    var deferred = Q.defer();

    getTopic(enhanceTopicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    email: contactEmails
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

function addContactEmailToPubSub(contactEmails) {
    var allPromises = [];

    var i, j, tempArray, chunk = 75;
    for (i = 0, j = contactEmails.length; i < j; i += chunk) {
        // Break array into a chunk
        tempArray = contactEmails.slice(i, i + chunk);
        var tempString = tempArray.join(',');

        // Execute contact sync
        var toExecute = addContactEmailsToPubSubTopicPublish(tempString);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function addContactInstagramUsernameToPubSubTopicPublish(contactInstagramUsernames) {
    var deferred = Q.defer();

    getTopic(newInstagramTopicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    username: contactInstagramUsernames
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

function addInstagramUsernameToPubSub(contactInstagramUsernames) {
    var allPromises = [];

    var i, j, tempArray, chunk = 75;
    for (i = 0, j = contactInstagramUsernames.length; i < j; i += chunk) {
        // Break array into a chunk
        tempArray = contactInstagramUsernames.slice(i, i + chunk);
        var tempString = tempArray.join(',');

        // Execute contact sync
        var toExecute = addContactInstagramUsernameToPubSubTopicPublish(tempString);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

function addContactTwitterUsernameToPubSubTopicPublish(contactTwitterUsernames) {
    var deferred = Q.defer();

    getTopic(newTwitterTopicName, function(err, topic) {
        if (err) {
            deferred.reject(new Error(err));
            console.error('Error occurred while getting pubsub topic', err);
            sentryClient.captureMessage(err);
        } else {
            topic.publish({
                data: {
                    username: contactTwitterUsernames
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

function addTwitterUsernameToPubSub(contactTwitterUsernames) {
    var allPromises = [];

    var i, j, tempArray, chunk = 75;
    for (i = 0, j = contactTwitterUsernames.length; i < j; i += chunk) {
        // Break array into a chunk
        tempArray = contactTwitterUsernames.slice(i, i + chunk);
        var tempString = tempArray.join(',');

        // Execute contact sync
        var toExecute = addContactTwitterUsernameToPubSubTopicPublish(tempString);
        allPromises.push(toExecute);
    }

    return Q.all(allPromises);
}

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {Object} requestData Cloud Function request data.
 * @param {string} requestData.Id Datastore ID string.
 * @returns {Object} Datastore key object.
 */
function getKeysFromRequestData(requestData, resouceType) {
    if (!requestData.Id) {
        throw new Error("Id not provided. Make sure you have a 'Id' property " +
            "in your request");
    }

    var ids = requestData.Id.split(',');
    var keys = [];

    for (var i = ids.length - 1; i >= 0; i--) {
        var contactId = parseInt(ids[i], 10);
        var datastoreId = datastore.key([resouceType, contactId]);
        keys.push(datastoreId);
    }

    return keys;
}

/**
 * Retrieves a record.
 *
 * @example
 * gcloud alpha functions call ds-get --data '{'kind':'gcf-test','key':'foobar'}'
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the user.
 * @param {string} data.kind The Datastore kind of the data to retrieve, e.g. 'user'.
 * @param {string} data.key Key at which to retrieve the data, e.g. 5075192766267392.
 */
function getDatastore(data, resouceType) {
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
 * Format a contact for ES sync
 *
 * @param {Object} contactData Contact details from datastore.
 */
function formatESContact(contactId, contactData) {
    contactData['Id'] = contactId;

    if ('CustomFields.Name' in contactData && 'CustomFields.Value' in contactData) {
        // Populate a column for contactData
        contactData['CustomFields'] = [];
        for (var i = contactData['CustomFields.Name'].length - 1; i >= 0; i--) {
            var singleData = {};
            singleData.Name = contactData['CustomFields.Name'][i];
            singleData.Value = contactData['CustomFields.Value'][i];
            contactData['CustomFields'].push(singleData);
        }

        // Remove the name and value fields
        delete contactData['CustomFields.Name'];
        delete contactData['CustomFields.Value'];
    }

    return contactData;
}

/**
 * Add a contact to ES
 *
 * @param {Object} contactData Contact details from datastore.
 * Returns true if adding data works and false if not.
 */
function addToElastic(contacts) {
    var deferred = Q.defer();
    var esActions = [];

    for (var i = contacts.length - 1; i >= 0; i--) {
        var contactId = contacts[i].key.id;
        var contactData = contacts[i].data;
        var postContactData = formatESContact(contactId, contactData);

        var indexRecord = {
            index: {
                _index: 'contacts',
                _type: 'contact',
                _id: contactId
            }
        };
        var dataRecord = postContactData;
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
 * Syncs a contact information to elasticsearch.
 *
 * @param {Object} contact Contact details from datastore.
 */
function getAndSyncElastic(contacts) {
    var deferred = Q.defer();

    addToElastic(contacts).then(function(status) {
        if (status) {
            var contactEmails = [];
            var contactTwitterUsernames = [];
            var contactInstagramUsernames = [];

            for (var i = 0; i < contacts.length; i++) {
                contactEmails.push(contacts[i].data.Email);

                if (contacts[i].data.Twitter !== '') {
                    contactTwitterUsernames.push(contacts[i].data.Twitter);
                }

                if (contacts[i].data.Instagram !== '') {
                    contactInstagramUsernames.push(contacts[i].data.Instagram);
                }
            }

            addContactEmailToPubSub(contactEmails).then(function(status) {
                addTwitterUsernameToPubSub(contactTwitterUsernames).then(function(status) {
                    addInstagramUsernameToPubSub(contactInstagramUsernames).then(function(status) {
                        deferred.resolve(true);
                    }, function (error) {
                        sentryClient.captureMessage(error);
                        deferred.reject(false);
                    });
                }, function (error) {
                    sentryClient.captureMessage(error);
                    deferred.reject(false);
                });
            }, function (error) {
                sentryClient.captureMessage(error);
                deferred.reject(false);
            });
        } else {
            deferred.resolve(false);
        }
    });

    return deferred.promise;
}

function removeContactFromElastic(contactId) {
    var deferred = Q.defer();

    var esActions = [];
    var eachRecord = {
        delete: {
            _index: 'contacts',
            _type: 'contact',
            _id: contactId
        }
    };
    esActions.push(eachRecord);

    client.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            deferred.resolve(false);
        } else {
            deferred.resolve(true);
        }
    });

    return deferred.promise;
}

function syncContact(data) {
    var deferred = Q.defer();
    if (data.Method && data.Method.toLowerCase() === 'create') {
        getDatastore(data, 'Contact').then(function(contacts) {
            if (contacts != null) {
                getAndSyncElastic(contacts).then(function(elasticResponse) {
                    if (elasticResponse) {
                        deferred.resolve('Success!');
                    } else {
                        var error = 'Elastic sync failed';
                        sentryClient.captureMessage(error);
                        deferred.reject(new Error(error));
                        throw new Error(error);
                    }
                });
            } else {
                var error = 'Contact not found';
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
                throw new Error(error);
            }
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
            throw new Error(error);
        });
    } else if (data.Method && data.Method.toLowerCase() === 'delete') {
        if (!data.Id) {
            throw new Error("Id not provided. Make sure you have a 'Id' property " +
                "in your request");
        }

        var contactId = parseInt(data.Id, 10);
        removeContactFromElastic(contactId).then(function(elasticResponse) {
            if (elasticResponse) {
                deferred.resolve('Success!');
            } else {
                var error = 'Elastic removal failed for ' + data.Id;
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
                throw new Error(error);
            }
        });
    } else {
        // This case should never happen unless wrong pub/sub method is called.
        var error = 'Can not parse method ' + data.Method;
        sentryClient.captureMessage(error);
        deferred.reject(new Error(error));
        throw new Error(error);
    }

    return deferred.promise;
}

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
    console.log('Received request to process contacts: ' + message.data.Id.length);
    syncContact(message.data)
        .then(function(status) {
            rp('https://hchk.io/d01a4dde-670b-4083-b749-e1bc1079d616')
                .then(function(htmlString) {
                    console.log('Completed execution for ' + message.data.Id.length);
                })
                .catch(function(err) {
                    console.error(err);
                });
        }, function(error) {
            sentryClient.captureMessage(error);
            console.error(error);
        });
});

/**
 * Triggered from a message on a Pub/Sub topic.
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the Pub/Sub trigger.
 * @param {Object} data.message Message that was published via Pub/Sub.
 */
exports.syncContacts = function syncContacts(data) {
    return syncContact(data);
};

function testSync(data) {
    return syncContact(data);
};

// testSync({Id: '6095325244686336', Method: 'create'})