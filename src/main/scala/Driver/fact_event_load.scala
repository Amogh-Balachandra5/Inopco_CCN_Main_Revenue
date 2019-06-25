package Driver

import java.text.SimpleDateFormat
import java.util.Calendar
import Load.Ccn_Main_Event_Load

object fact_event_load extends App  {

  var date_enter = ""
  if(0 < args.length)
    date_enter = args(0)
  else
  {
    var cal = Calendar.getInstance()
    val date_format = new SimpleDateFormat("yyyy-MM-dd")
    cal.add(Calendar.DATE, -1)
    date_enter = date_format.format(cal.getTime)
  }
  new Ccn_Main_Event_Load().CCN_Main_load()
}
