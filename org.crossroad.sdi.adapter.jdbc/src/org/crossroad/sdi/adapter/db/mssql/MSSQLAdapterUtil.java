package org.crossroad.sdi.adapter.db.mssql;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.sap.hana.dp.adapter.sdk.AdapterException;

public class MSSQLAdapterUtil {


    private MSSQLAdapterUtil()
    {
    }

    public static String str2DT(String _val)
    {
        StringBuffer _res = new StringBuffer();
        int index = -1;
        if(_val != null)
        {
            index = _val.indexOf('.');
            if(index > 0)
            {
                if(_val.length() - index > 4)
                {
                    _res.append(_val.substring(0, index));
                    _res.append('.');
                    _res.append(_val.substring(++index, index + 3));
                    _res.append("'");
                } else
                {
                    _res.append(_val);
                }
            } else
            {
                _res.append(_val);
            }
        }
        return _res.toString();
    }
    
    public static String buidTS(String value) throws AdapterException
    {
    	Calendar today = Calendar.getInstance();
    	Calendar dt = Calendar.getInstance();
    	StringBuffer buffer = new StringBuffer();
    	SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    	
    	try {
    		value = value.replaceAll("\'", "");
    		dt.setTime(fmt.parse(value));
    		if (String.valueOf(dt.get(Calendar.YEAR)).length() < 4 )
    		{
    			fmt = new SimpleDateFormat("yyyy-MM-dd");
    			String day = fmt.format(today.getTime());
    			
    			fmt = new SimpleDateFormat("HH:mm:ss.sss");
    			String hours = fmt.format(dt.getTime());

    			buffer.setLength(0);
    			buffer.append("'").append(day);
    			buffer.append(' ');
    			buffer.append(hours).append("'");
    		} else {
    			
    			buffer.append("'").append(fmt.format(dt.getTime())).append("'");
    		}
    		
    		
    	} catch(Exception e)
    	{
    		throw new AdapterException(e);
    	}
    	
    	
    	return buffer.toString();
    }
    
    
    public static void main(String[] args) {
		try {
			System.out.println("2016-06-11 12:00:00.000000 >>> " + buidTS("2016-06-11 12:00:00.000000"));
		} catch (AdapterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			System.out.println("2016-06-11 12:00:00.000 >>> " +buidTS("'''2016-06-11 12:00:00.000"));
		} catch (AdapterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			System.out.println("0001-01-01 12:00:00.000 >>> "+ buidTS("0001-01-01 12:50:00.000"));
		} catch (AdapterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
