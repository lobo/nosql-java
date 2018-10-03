package helper;

import java.io.Serializable;

public class VertexProp implements Serializable
{
    private String URL;
    private String creator;


    public VertexProp(String anUrl, String aCreator)
    {
        URL= anUrl;
        creator= aCreator;
    }

    public String getURL()
    {
        return URL;
    }

    public String getCreator()
    {
        return creator;
    }

}
