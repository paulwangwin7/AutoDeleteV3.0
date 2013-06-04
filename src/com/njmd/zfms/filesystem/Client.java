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
 * "ִ����¼��"����ϵͳ֮��ʱɾ���ļ��ͻ���
 * @author ��ǿΰ
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
	
	//���ݿ�����
	private String jdbcDriverName=null;
	//���ݿ����ӵ�ַ
	private String jdbcUrl=null;
	//���ݿ��û���
	private String jdbcUsername=null;
	//���ݿ�����
	private String jdbcPassword=null;
	//ɾ������ʱ,ÿ��ɾ���ļ��ļ�¼��
	private String jdbcBatchSize=null;
	//�ļ���������IP��ַ,���ڻ�ȡ�������ܱ���ļ�¼
	private String fileServerIP=null;
	
	//���й������������ݿ�ʧ��֮��ȴ��೤ʱ��֮����ִ��ɾ������(��λ����)
	private Integer intervalFailsRetry=null;
	//����ɾ���������֮�����´�����ɾ������֮��ĵȴ�ʱ��(��λ����)
	private Integer intervalEachTime=null;
	
	/**
	 * �������еĹ����л�����.lock���ļ�,���³���ʵ������ʱ�����Ż�ȡ.lock�ļ�����,
	 * �����ȡ����������˵��������һ������ʵ�������У���ʱ��ʵ����ֱ���˳�.
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
			     // ���û�еõ�����������˳�.
			     // û�б�Ҫ�ֶ��ͷ����͹ر������������˳�ʱ�����ǻᱻ�رյ�.
			     throw new Exception("An instance of the application is running.");
			 }
		} catch (Exception e) {
			logger.error("��⵽����������һ������ʵ���������л�����һ������û�������ر�,����ͬĿ¼���Ƿ����.lock�ļ�,���������ֱ��ɾ��֮�������б�����,���û�����ٴ����б����򼴿�...");
			System.exit(0);
		}
	}
	
	/**
	 * ����ϵͳ�е���ز������г�ʼ����������֤.
	 */
	public void init(){
		logger.info("********************************************");
		logger.info("\"ִ����¼��\"����ϵͳ֮��ʱɾ���ļ��ͻ��˿�ʼ����...");
		logger.info("********************************************");
		logger.info("ϵͳ���ڽ��м��,���Ժ�...");

		lockFile();
		
		logger.info("ϵͳ���ڳ�ʼ�����ò���...");
		
		//��ȡ�����ļ�
		Properties props=new Properties();
		try{
			File file=new File("conf/application.properties");
			InputStream in=(InputStream) new FileInputStream(file);
			props.load(in);
			logger.info("ϵͳ�ɹ��ҵ�[conf/application.properties]�����ļ�...");
		}catch(IOException e){
			logger.error("ϵͳδ�ҵ�[conf/application.properties]�����ļ�,�����Զ��˳�...");
			System.exit(0);
		}
		
		jdbcDriverName=props.getProperty(JDBC_DRIVERNAME, "").trim();
		if("".equals(jdbcDriverName)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������jdbc.driverName��ֵ...");
			System.exit(0);
		}
		
		jdbcUrl=props.getProperty(JDBC_URL,"").trim();
		if("".equals(jdbcUrl)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������jdbc.url��ֵ...");
			System.exit(0);
		}
		
		jdbcUsername=props.getProperty(JDBC_USERNAME,"").trim();
		if("".equals(jdbcUsername)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������jdbc.username��ֵ...");
			System.exit(0);
		}
		
		jdbcPassword=props.getProperty(JDBC_PASSWORD,"").trim();
		if("".equals(jdbcPassword)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������jdbc.password��ֵ...");
			System.exit(0);
		}
		
		jdbcBatchSize=props.getProperty(JDBC_BATCHSIZE,""+Integer.MAX_VALUE).trim();
		if(!isDigits(jdbcBatchSize)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ��jdbc.password��ֵ�Ƿ�Ϊ����...");
			System.exit(0);
		}
		
		fileServerIP=props.getProperty(FILESERVER_IP,"").trim();
		if("".equals(fileServerIP)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������fileserver.ip��ֵ...");
			System.exit(0);
		}
			
		String tmpIntervalFailsRetry=props.getProperty(INTERVAL_FAILSRETRY,"5").trim();
		if("".equals(tmpIntervalFailsRetry)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������interval.failsRetry��ֵ...");
			System.exit(0);
		}
		
		if(!isDigits(tmpIntervalFailsRetry)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ��interval.failsRetry��ֵ�Ƿ�Ϊ����...");
			System.exit(0);
		}
		
		intervalFailsRetry=Integer.valueOf(tmpIntervalFailsRetry);
		
		String tmpIntervalEachTime=props.getProperty(INTERVAL_EACHTIME,"5").trim();
		if("".equals(tmpIntervalEachTime)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ���Ƿ�������interval.eachTime��ֵ...");
			System.exit(0);
		}
		
		if(!isDigits(tmpIntervalEachTime)){
			logger.error("��ȡ�����ļ����ݳ���,�����Զ��˳�,����ȷ��interval.eachTime��ֵ�Ƿ�Ϊ����...");
			System.exit(0);
		}
		
		intervalEachTime=Integer.valueOf(tmpIntervalEachTime);
		
		logger.info("ϵͳ��ȡ[conf/application.properties]�����ļ���Ϣ�ɹ�...");
		
	}
	
	/**
	 * �������ݿ�����
	 */
	public void loadDriver(){
		try {
			Class.forName(jdbcDriverName);
		} catch (ClassNotFoundException e) {
			logger.error("�������ݿ�����ʧ��,�����Զ��˳�,��ʹ��Ŀǰ֧�ֵ�Oracle���ݿ�...");
			System.exit(0);
		}
	}
	
	/**
	 * �����õ����ݿ������Ϣ���м������
	 */
	public void testConnection(){
		Connection conn =null;
		try {
			conn=DriverManager.getConnection(jdbcUrl,jdbcUsername,jdbcPassword);
			conn.close();
		} catch (SQLException e) {
			logger.error("�������ݿ�ʧ��,�����Զ��˳�,��ȷ�������õ����ݿ��û����������Ƿ���ȷ...");
			System.exit(0);
		}
	}
	
	/**
	 * ע��ϵͳ�ر�hook
	 */
	public void registerShutdownHook(){
		if(shutdownHook==null){
			shutdownHook=new ShutdownHook();
		}
		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}
	
	/**
	 * �����ļ�ɾ���߳�
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
     * JVM�ر�hook�̣߳����������д����а���ctrl+C�жϳ���ִ��ʱ����ִ��Щ����������
     * ���Ƕ�ֱ�ӹرմ��ڽ���Hook
     *
     */
	protected class ShutdownHook extends Thread{
		public void run(){
			try{
				logger.info("ϵͳ�����˳�,���Ժ�...");
				if(null!=t)
					t.setStop(true); //�����߳��˳���־
				if(null!=executor)
					executor.shutdown();	
				logger.info("ϵͳ���˳�...");
			}catch(Throwable ex){
				ex.printStackTrace();
			}finally{
				
			}
		}
	}

	/**
	 * �ļ�ɾ���̣߳����������ȶ��ļ�������л�ã��ٸ��������ʱ���ȡ�Ѿ����ڵ��ļ���¼��
	 * ������ھ�ɾ������ļ��������ļ���¼״̬
	 *
	 */
	protected class DeleteThread  implements Runnable{
		
		//�Ƿ������־
		private boolean isStop=false;
		
		public void setStop(boolean isStop) {
			this.isStop = isStop;
		}
		
		@Override
		public void run() {
			while(!isStop){
				logger.info("ִ��ɾ������...");
			 	
				Connection conn=null;
				ResultSet rs=null;
				try {
					//��ȡ�ļ������б�
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
			 			//�Է���ֱ�����ļ�ɾ������
			 			for(Map.Entry<String, Integer> entry:types.entrySet()){
			 				//���ݵ�ǰ�����Լ�����������ù��ڵ�ʱ��
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
					 				logger.info("ϵͳ��ʾ:�ļ�["+fileSavePath+"]ɾ���ɹ�,���ļ���¼Ϊ["+rs.getString("file_id")+"]!");
				 				}catch(Exception e){
				 					logger.info("ϵͳ��ʾ:�ļ�["+fileSavePath+"]ɾ��ʧ��,���ļ���¼Ϊ["+rs.getString("file_id")+"]!");
				 					throw e;
				 				}
			 				
				 				String filePlayPath=rs.getString("file_play_path");
				 				filePlayPath=fileStorageRoot+filePlayPath;
				 				try{
					 				if(new File(filePlayPath).exists()){
					 					new File(filePlayPath).delete();
					 				}
					 				logger.info("ϵͳ��ʾ:�ļ�["+filePlayPath+"]ɾ���ɹ�,���ļ���¼Ϊ["+rs.getString("file_id")+"]!");
				 				}catch(Exception e){
				 					logger.info("ϵͳ��ʾ:�ļ�["+fileSavePath+"]ɾ��ʧ��,���ļ���¼Ϊ["+rs.getString("file_id")+"]!");
				 					throw e;
				 				}
			 				
				 				String fileShowPath=rs.getString("file_show_path");
				 				fileShowPath=fileStorageRoot+fileShowPath;
				 				try{
					 				if(new File(fileShowPath).exists()){
					 					new File(fileShowPath).delete();
					 				}
					 				logger.info("ϵͳ��ʾ:�ļ�["+fileShowPath+"]ɾ���ɹ�,���ļ���¼Ϊ["+rs.getString("file_id")+"]!");
				 				}catch(Exception e){
				 					logger.info("ϵͳ��ʾ:�ļ�["+fileShowPath+"]ɾ��ʧ��,���ļ���¼Ϊ["+rs.getString("file_id")+"]!");
				 					throw e;
				 				}
			 				
				 				try {
									conn.createStatement().execute("update file_upload_info set delete_by='0' , delete_time='"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())+"' , file_status='U' where file_id='"+rs.getString("file_id")+"'");
									logger.info("ϵͳ��ʾ:�����ļ���¼["+rs.getString("file_id")+"]ΪʧЧ״̬�Ĳ���ִ�гɹ�!");
				 				} catch (Exception e) {
				 					logger.error("ϵͳ��ʾ:�����ļ���¼["+rs.getString("file_id")+"]ΪʧЧ״̬�Ĳ���ִ��ʧ��!");
				 					throw e;
				 				}
			 				
			 				}catch(Exception e){
			 					error++;
			 				}
			 			}
			 			try { rs.close(); } catch (Exception e) {}
			 			logger.info("ϵͳ��ʾ:����ִ��ɾ������,һ����ѯ����Ҫɾ��"+total+"����¼,����"+error+"����¼ɾ��ʧ��!");
			 		}else{
			 			logger.info("ϵͳ��ʾ:����ִ��ɾ������,û�з�����Ҫɾ�����ļ���¼!");
			 		}
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("��ѯ���ݿ���Ϣʧ��,�������ݿ��������״̬,ϵͳ����"+intervalFailsRetry+"����֮������...");
					int tmp=intervalFailsRetry*60;
					while(!isStop && tmp>0){
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
						} finally{
							tmp--;
							if(tmp % 60 ==0)
								logger.error("��ѯ���ݿ���Ϣʧ��,�������ݿ��������״̬,ϵͳ����"+(tmp/60)+"����֮������...");
						}
					}
				}finally{
					if(null!=conn)
						try{ conn.close(); }catch(Exception e){}
					
					if(null!=rs)
						try{ rs.close(); }catch(Exception e){}
				}
				int tmp=intervalEachTime*60;
				logger.info("����ִ��ɾ���������,�´�ִ��ɾ��������"+(tmp/60)+"����֮��ִ��...");
				while(!isStop && tmp>0){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
					} finally{
						tmp--;
						if(tmp % 60 ==0)
							logger.info("����ִ��ɾ���������,�´�ִ��ɾ��������"+(tmp/60)+"����֮��ִ��...");
						
					}
				}
			}
		}
	}
	
	/**
	 * ���������
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
