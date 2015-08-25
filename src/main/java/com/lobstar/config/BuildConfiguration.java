package com.lobstar.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;

import com.lobstar.base.log.XLogger;

/**
 * 构建参数，设置不可变的参数值
 * @author lobster
 *
 */
public class BuildConfiguration {

    private static Logger LOG = XLogger.getLogger(BuildConfiguration.class);

    public BuildConfiguration buildConfig() throws IOException {
        String file = "taskeeper-config.xml";
        InputStream stream = null;
        String basePath = BuildConfiguration.class.getClassLoader().getResource("").getPath();
        File conf = new File(basePath + "/conf/" + file);
        LOG.info("xxx-file: "+conf.getAbsolutePath());
        if (conf.exists()) {
            stream = new FileInputStream(conf);
        } else {
            stream = BuildConfiguration.class.getClassLoader().getResourceAsStream(file);
        }
        if (stream != null) {
            SAXReader reader = new SAXReader();
            try {
                Document document = reader.read(stream);
                Element root = document.getRootElement();

                @SuppressWarnings("unchecked")
                List<Element> zoos = root.selectNodes("zoo");
                if (zoos.size() > 0) {
                    this.zooHost = zoos.get(0).element("ip").getStringValue();
                    this.zooPort = Integer.parseInt(zoos.get(0).element("port").getStringValue());
                }

                @SuppressWarnings("unchecked")
                List<Element> indices = root.selectNodes("index");
                if (indices.size() > 0) {
                    this.esHost = indices.get(0).element("ip").getStringValue();
                    this.esPort = Integer.parseInt(indices.get(0).element("port").getStringValue());
                    this.esName = indices.get(0).element("name").getStringValue();
                }

                @SuppressWarnings("unchecked")
                List<Element> ticket = root.selectNodes("ticket");
                if (ticket.size() > 0) {
                    this.setTicketHost(ticket.get(0).element("ip").getStringValue());
                    this.setTicketPort(Integer.parseInt(ticket.get(0).element("port").getStringValue()));
                }

            } catch (DocumentException e) {
                LOG.error(e.getMessage(),e);
            }

        }

        return this;
    }

    private String zooHost;
    private String esHost;
    private int zooPort;
    private int esPort;

    private String ticketHost;

    private int ticketPort;

    private String esName;

    public String getZooHost() {
        return zooHost;
    }

    public void setZooHost(String zooHost) {
        this.zooHost = zooHost;
    }

    public String getEsHost() {
        return esHost;
    }

    public void setEsHost(String esHost) {
        this.esHost = esHost;
    }

    public int getZooPort() {
        return zooPort;
    }

    public void setZooPort(int zooPort) {
        this.zooPort = zooPort;
    }

    public int getEsPort() {
        return esPort;
    }

    public void setEsPort(int esPort) {
        this.esPort = esPort;
    }

    public String getEsName() {
        return esName;
    }

    public void setEsName(String esName) {
        this.esName = esName;
    }

    public String getTicketHost() {
        return ticketHost;
    }

    public void setTicketHost(String ticketHost) {
        this.ticketHost = ticketHost;
    }

    public int getTicketPort() {
        return ticketPort;
    }

    public void setTicketPort(int ticketPort) {
        this.ticketPort = ticketPort;
    }

}
