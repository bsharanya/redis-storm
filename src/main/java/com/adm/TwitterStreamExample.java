package com.adm;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamExample {

    public static void main(String[] args) throws TwitterException {
        ConfigurationBuilder configuration = new ConfigurationBuilder();
        configuration.setOAuthConsumerKey("************************")
                .setOAuthConsumerSecret("************************")
                .setOAuthAccessToken("************************")
                .setOAuthAccessTokenSecret("************************");

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                System.out.println(status.getUser().getName() + " : " + status.getText());
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            @Override
            public void onTrackLimitationNotice(int i) {}

            @Override
            public void onScrubGeo(long l, long l2) {}

            @Override
            public void onStallWarning(StallWarning stallWarning) {}

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
        };

        TwitterStreamFactory streamFactory = new TwitterStreamFactory(configuration.build());
        TwitterStream stream = streamFactory.getInstance();
        stream.addListener(listener);
        stream.sample();
    }
}
