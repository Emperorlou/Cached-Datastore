# Cached-Datastore
This project is a framework built on top of the regular Appengine Datastore interfaces and is used to seamlessly utilize Memcache while mimicking the native datastore interfaces. It also includes some utilities to make a variety of common tasks easier.

# Eclipse installation
I use the GWT plugin for eclipse personally, but really you just need the appengine SDK. In addition to the core appengine SDK, you will also need 2 Remote API jars. Remote API is used when debugging locally so that access to access the live data from your local box.

To add the required Remote API jars for use inside the application, add ${SDK_ROOT}/lib/impl/appengine-api.jar and ${SDK_ROOT}/lib/appengine-remote-api.jar to your classpath. The .classpath that comes with the repo already has entries for this, so you will need to update their paths and leave it uncommitted (yes I know the lack of a proper build process is disturbing).
