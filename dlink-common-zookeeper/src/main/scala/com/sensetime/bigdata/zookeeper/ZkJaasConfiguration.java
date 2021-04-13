package com.sensetime.bigdata.zookeeper;

import com.google.common.collect.ImmutableMap;
import org.apache.zookeeper.client.ZKClientConfig;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author zhangqiang
 * @since 2020/6/24 13:22
 */
public class ZkJaasConfiguration extends Configuration {

    private String username;
    private String password;

    public ZkJaasConfiguration(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Retrieve the AppConfigurationEntries for the specified <i>name</i>
     * from this Configuration.
     *
     * <p>
     *
     * @param name the name used to index the Configuration.
     * @return an array of AppConfigurationEntries for the specified <i>name</i>
     * from this Configuration, or null if there are no entries
     * for the specified <i>name</i>
     */
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        // Create entry options.
        Map<String, Object> options = ImmutableMap.of(
                "username", username,
                "password", password
        );
        checkArgument(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY_DEFAULT.equals(name));
        return new AppConfigurationEntry[]{
                new AppConfigurationEntry(
                        "org.apache.zookeeper.server.auth.DigestLoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options)
        };
    }
}
