package helper;

import java.io.Serializable;
import java.util.Date;

public class EdgeProp implements Serializable
{
    private String label;
    private Date date;

    public EdgeProp(String aLabel, Date aDate )
    {
        label= aLabel;
        date= aDate;
    }

    public String getLabel()
    {
        return label;
    }

    public Date getDate()
    {
        return date;
    }

}
