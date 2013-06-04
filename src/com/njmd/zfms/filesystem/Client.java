package com.njmd.zfms.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * "执法记录仪"管理系统之定时删除文件客户端
 * @author 孙强伟
 * @version 0.0.1
 * @since 2013.05.29
 */
public class Client{
	private static Log logger= LogFactory.getLog(Client.class);
	
	private Thread shutdownHook=null;
	private ScheduledExecutorService  executor =null;
	private DeleteThread t=null;
	
	private static final String JDBC_DRIVERNAME="jdbc.driverName";
	private static final String JDBC_URL="jdbc.url";
	private static final String JDBC_USERNAME="jdbc.username";
	private static final String JDBC_PASSWORD="jdbc.password";
	private static final String JDBC_BATCHSIZE="jdbc.batchSize";
	
	private static final String FILESERVER_IP="fileserver.ip";
	 
	private static final String INTERVAL_FAILSRETRY="interval.failsRetry";
	private static final String INTERVAL_EACHTIME="interval.eachTime";
	
	//数据库驱动
	private String jdbcDriverName=null;
	//数据库连接地址
	private String jdbcUrl=null;
	//数据库用户名
	private String jdbcUsername=null;
	//数据库密码
	private String jdbcPassword=null;
	//删除任务时,每次删除文件的记录数
	private String jdbcBatchSize=null;
	//文件服务器的IP地址,用于获取本机可能保存的记录
	private String fileServerIP=null;
	
	//运行过程中连接数据库失败之后等待多长时间之后再执行删除任务(单位分钟)
	private Integer intervalFailsRetry=null;
	//本次删除任务完成之后与下次运行删除任务之后的等待时间(单位分钟)
	private Integer intervalEachTime=null;
	
	/**
	 * 程序运行的过程中会生成.lock锁文件,当新程序实例运行时会试着获取.lock文件的锁,
	 * 如果获取不到锁，则说明有另外一个程序实例在运行，此时本实例就直接退出.
	 */
	public void lockFile(){
		try {
			File f=new File(".lock");
			f.deleteOnExit();
			f.createNewFile();
			RandomAccessFile  raf = new RandomAccessFile(f, "rw");
			FileChannel  channel = raf.getChannel();
			FileLock lock = channel.tryLock();

			 if (lock == null) {
			     // 如果没有得到锁，则程序退出.
			     // 没有必要手动释放锁和关闭流，当程序退出时，他们会被关闭的.
			     throw new Exception("An instance of the application is running.");
			 }
		} catch (Exception e) {
			logger.error("检测到可能有另外一个程序实例正在运行或者上一次运行没有正常关闭,请检查同目录下是否存在.lock文件,如果存在则直接删除之后再运行本程序,如果没有则再次运行本程序即可...");
			System.exit(0);
		}
	}
	
	/**
	 * 进行系统中的相关参数进行初始化并进行验证.
	 */
	public void init(){
		logger.info("********************************************");
		logger.info("\"执法记录仪\"管理系统之定时删除文件客户端开始启动...");
		logger.info("********************************************");
		logger.info("系统正在进行检查,请稍后...");

		lockFile();
		
		logger.info("系统正在初始化配置参数...");
		
		//读取配置文件
		Properties props=new Properties();
		try{
			File file=new File("conf/application.properties");
			InputStream in=(InputStream) new FileInputStream(file);
			props.load(in);
			logger.info("系统成功找到[conf/application.properties]配置文件...");
		}catch(IOException e){
			logger.error("系统未找到[conf/application.properties]配置文件,程序自动退出...");
			System.exit(0);
		}
		
		jdbcDriverName=props.getProperty(JDBC_DRIVERNAME, "").trim();
		if("".equals(jdbcDriverName)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了jdbc.driverName的值...");
			System.exit(0);
		}
		
		jdbcUrl=props.getProperty(JDBC_URL,"").trim();
		if("".equals(jdbcUrl)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了jdbc.url的值...");
			System.exit(0);
		}
		
		jdbcUsername=props.getProperty(JDBC_USERNAME,"").trim();
		if("".equals(jdbcUsername)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了jdbc.username的值...");
			System.exit(0);
		}
		
		jdbcPassword=props.getProperty(JDBC_PASSWORD,"").trim();
		if("".equals(jdbcPassword)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了jdbc.password的值...");
			System.exit(0);
		}
		
		jdbcBatchSize=props.getProperty(JDBC_BATCHSIZE,""+Integer.MAX_VALUE).trim();
		if(!isDigits(jdbcBatchSize)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认jdbc.password的值是否为数字...");
			System.exit(0);
		}
		
		fileServerIP=props.getProperty(FILESERVER_IP,"").trim();
		if("".equals(fileServerIP)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了fileserver.ip的值...");
			System.exit(0);
		}
			
		String tmpIntervalFailsRetry=props.getProperty(INTERVAL_FAILSRETRY,"5").trim();
		if("".equals(tmpIntervalFailsRetry)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了interval.failsRetry的值...");
			System.exit(0);
		}
		
		if(!isDigits(tmpIntervalFailsRetry)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认interval.failsRetry的值是否为数字...");
			System.exit(0);
		}
		
		intervalFailsRetry=Integer.valueOf(tmpIntervalFailsRetry);
		
		String tmpIntervalEachTime=props.getProperty(INTERVAL_EACHTIME,"5").trim();
		if("".equals(tmpIntervalEachTime)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认是否设置了interval.eachTime的值...");
			System.exit(0);
		}
		
		if(!isDigits(tmpIntervalEachTime)){
			logger.error("读取配置文件内容出错,程序自动退出,请您确认interval.eachTime的值是否为数字...");
			System.exit(0);
		}
		
		intervalEachTime=Integer.valueOf(tmpIntervalEachTime);
		
		logger.info("系统读取[conf/application.properties]配置文件信息成功...");
		
	}
	
	/**
	 * 加载数据库驱动
	 */
	public void loadDriver(){
		try {
			Class.forName(jdbcDriverName);
		} catch (ClassNotFoundException e) {
			logger.error("加载数据库驱动失败,程序自动退出,请使用目前支持的Oracle数据库...");
			System.exit(0);
		}
	}
	
	/**
	 * 对配置的数据库相关信息进行检测连接
	 */
	public void testConnection(){
		Connection conn =null;
		try {
			conn=DriverManager.getConnection(jdbcUrl,jdbcUsername,jdbcPassword);
			conn.close();
		} catch (SQLException e) {
			logger.error("连接数据库失败,程序自动退出,请确认您设置的数据库用户名和密码是否正确...");
			System.exit(0);
		}
	}
	
	/**
	 * 注册系统关闭hook
	 */
	public void registerShutdownHook(){
		if(shutdownHook==null){
			shutdownHook=new ShutdownHook();
		}
		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
	
	/**
	 * 开启文件删除线程
	 */
	public void start(){
		executor=Executors.newSingleThreadScheduledExecutor();
		t=new DeleteThread();
		executor.submit(t);
	}
	
    public static boolean isDigits(String str) {
        if (str == null || str.length() == 0) {
            return false;
        }
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        
        return true;
    }
	
    /**
     * JVM关闭hook线程，当在命令行窗口中按下ctrl+C中断程序执行时，会执行些方法，但是
     * 不是对直接关闭窗口进行Hook
     *
     */
	protected class ShutdownHook extends Thread{
		public void run(){
			try{
				logger.info("系统正在退出,请稍后...");
				if(null!=t)
					t.setStop(true); //设置线程退出标志
				if(null!=executor)
					executor.shutdown();	
				logger.info("系统已退出...");
			}catch(Throwable ex){
				ex.printStackTrace();
			}finally{
				
			}
		}
	}

	/**
	 * 文件删除线程，其流程是先对文件分类进行获得，再根据其过期时间获取已经过期的文件记录，
	 * 如果存在就删除相关文件及更新文件记录状态
	 *
	 */
	protected class DeleteThread  implements Runnable{
		
		//是否结束标志
		private boolean isStop=false;
		
		public void setStop(boolean isStop) {
			this.isStop = isStop;
		}
		
		@Override
		public void run() {
			while(!isStop){
				logger.info("执行删除任务...");
			 	
				Connection conn=null;
				ResultSet rs=null;
				try {
					//获取文件分类列表
					Map<String,Integer> types=new HashMap<String,Integer>();
					conn=DriverManager.getConnection(jdbcUrl,jdbcUsername,jdbcPassword);
					rs=conn.createStatement().executeQuery("select * from file_type_info where valid_time > 0");
					while(rs.next()){
						String id=rs.getString("type_id");
						Integer time=rs.getInt("valid_time");
						if(time>0)
							types.put(id, time);
					}
					try { rs.close(); } catch (Exception e) {}
					
			 		if(types.size()>0){
			 			int total=0;
			 			int error=0;
			 			StringBuilder sb=new StringBuilder();
			 			//对分类分别进行文件删除操作
			 			for(Map.Entry<String, Integer> entry:types.entrySet()){
			 				//根据当前日期以及过期天数获得过期的时间
			 				Calendar calendar=Calendar.getInstance();
			 				calendar.add(Calendar.DAY_OF_MONTH, 0-entry.getValue());
			 				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			 				String time = sdf.format(calendar.getTime());
			 				sb.append("(file_upload_time <  '"+time+"' and type_id='"+entry.getKey()+"') ").append(" or ");
			 			}
			 			if(sb.length()>0)
			 				sb.delete(sb.length()-1-"or ".length(), sb.length()-1);
			 			
			 			rs=conn.createStatement().executeQuery("select * from(" +
			 					"select * from file_upload_info where file_context_path like '%"+fileServerIP+"%' and file_status!='U' and ("+sb.toString()+") " +
			 							" order by file_upload_time) where rownum<="+jdbcBatchSize+" ");
			 			while(rs.next()){
			 				total++;
			 				try{
			 					String fileStorageRoot=rs.getString("file_storage_root");
			 					
				 				String fileSavePath=rs.getString("file_save_path");
				 				fileSavePath=fileStorageRoot+fileSavePath;
				 				try{
					 				if(new File(fileSavePath).exists()){
					 					new File(fileSavePath).delete();
					 				}
					 				logger.info("系统提示:文件["+fileSavePath+"]删除成功,其文件记录为["+rs.getString("file_id")+"]!");
				 				}catch(Exception e){
				 					logger.info("系统提示:文件["+fileSavePath+"]删除失败,其文件记录为["+rs.getString("file_id")+"]!");
				 					throw e;
				 				}
			 				
				 				String filePlayPath=rs.getString("file_play_path");
				 				filePlayPath=fileStorageRoot+filePlayPath;
				 				try{
					 				if(new File(filePlayPath).exists()){
					 					new File(filePlayPath).delete();
					 				}
					 				logger.info("系统提示:文件["+filePlayPath+"]删除成功,其文件记录为["+rs.getString("file_id")+"]!");
				 				}catch(Exception e){
				 					logger.info("系统提示:文件["+fileSavePath+"]删除失败,其文件记录为["+rs.getString("file_id")+"]!");
				 					throw e;
				 				}
			 				
				 				String fileShowPath=rs.getString("file_show_path");
				 				fileShowPath=fileStorageRoot+fileShowPath;
				 				try{
					 				if(new File(fileShowPath).exists()){
					 					new File(fileShowPath).delete();
					 				}
					 				logger.info("系统提示:文件["+fileShowPath+"]删除成功,其文件记录为["+rs.getString("file_id")+"]!");
				 				}catch(Exception e){
				 					logger.info("系统提示:文件["+fileShowPath+"]删除失败,其文件记录为["+rs.getString("file_id")+"]!");
				 					throw e;
				 				}
			 				
				 				try {
									conn.createStatement().execute("update file_upload_info set delete_by='0' , delete_time='"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())+"' , file_status='U' where file_id='"+rs.getString("file_id")+"'");
									logger.info("系统提示:更新文件记录["+rs.getString("file_id")+"]为失效状态的操作执行成功!");
				 				} catch (Exception e) {
				 					logger.error("系统提示:更新文件记录["+rs.getString("file_id")+"]为失效状态的操作执行失败!");
				 					throw e;
				 				}
			 				
			 				}catch(Exception e){
			 					error++;
			 				}
			 			}
			 			try { rs.close(); } catch (Exception e) {}
			 			logger.info("系统提示:本次执行删除任务,一共查询到需要删除"+total+"条记录,其中"+error+"条记录删除失败!");
			 		}else{
			 			logger.info("系统提示:本次执行删除任务,没有发现需要删除的文件记录!");
			 		}
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("查询数据库信息失败,请检查数据库服务器的状态,系统将在"+intervalFailsRetry+"分钟之后再试...");
					int tmp=intervalFailsRetry*60;
					while(!isStop && tmp>0){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
						} finally{
							tmp--;
							if(tmp % 60 ==0)
								logger.error("查询数据库信息失败,请检查数据库服务器的状态,系统将在"+(tmp/60)+"分钟之后再试...");
						}
					}
				}finally{
					if(null!=conn)
						try{ conn.close(); }catch(Exception e){}
					
					if(null!=rs)
						try{ rs.close(); }catch(Exception e){}
				}
				int tmp=intervalEachTime*60;
				logger.info("本次执行删除任务完成,下次执行删除任务将在"+(tmp/60)+"分钟之后执行...");
				while(!isStop && tmp>0){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					} finally{
						tmp--;
						if(tmp % 60 ==0)
							logger.info("本次执行删除任务完成,下次执行删除任务将在"+(tmp/60)+"分钟之后执行...");
						
					}
				}
			}
		}
	}
	
	/**
	 * 主程序入口
	 */
	public static void main(String[] args) {
		Client client=new Client();
		client.init();
		client.loadDriver();
		client.testConnection();
		client.start();
		client.registerShutdownHook();
	}
}
