package com.yr.router;

import java.util.*;

import com.yr.common.Register;
import com.yr.handle.Handler;
import com.yr.vo.Event;
import com.yr.router.strategy.HbaseConnection;
import com.yr.router.strategy.Router;
import com.yr.router.strategy.StrategyType;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yr.conf.ConfigurationManager;
import com.yr.constant.Constants;

import scala.Serializable;

/**
 * Description 消息处理入口
 * @return
 * @Author dengbp
 * @Date 13:58 2020-03-10
 **/
public class CustomForeachWriter extends ForeachWriter<Row> implements Register<Handler>,
		Serializable {
	private static Logger logger = LoggerFactory.getLogger(CustomForeachWriter.class);
	private Set<Handler> handlers = new HashSet<>();
	private Event event;
	private final Router router;

	static {
		logger.info("初始化数据存储策略...");
		String[] routingAll = ConfigurationManager.getProperty(Constants.DATA_WRITE_ROUTING).split(Constants.DATA_WRITE_ROUTING_SPLIT);
		for (String routing:routingAll){
			StrategyType strategyType = StrategyType.getByCode(routing);
			switch (strategyType) {
				case HBASESTRATEGY:
					Router.register(new HbaseConnection());
					break;
				case MYSQLSTRATEGY:
					logger.warn("need to add mysql strategy...");
					break;
				default:
					logger.warn("strategyType[{}] is invalid",strategyType);
			}
		}
	}

	public CustomForeachWriter(Router router) {
		this.router = router;
	}

	/**
	 * Description 当有数据过来会触发
	 * @param partitionId
 * @param version
	 * @return boolean
	 * @Author dengbp
	 * @Date 18:59 2020-03-12
	 **/
	@Override
	public boolean open(long partitionId, long version) {
		logger.info("call ForeachWriter open method...");
		return router.init();
	}

	/**
	 * Description 当open方法执行后触发
	 * @param value
	 * @return void
	 * @Author dengbp
	 * @Date 18:59 2020-03-12
	 **/
	@Override
	public void process(Row value) {
		this.event = new Event(value);
		this.notifyAllHandler();
	}

	/**
	 * Description 每处理完 Row后会触发
	 * @param errorOrNull
	 * @return void
	 * @Author dengbp
	 * @Date 19:00 2020-03-12
	 **/
	@Override
	public void close(Throwable errorOrNull) {
		if (errorOrNull != null){
			logger.error(errorOrNull.getMessage());
		}
	}

	@Override
	public void signUp(Handler handler) {
		handlers.add(handler);
	}

	@Override
	public void notifyAllHandler() {
		handlers.forEach(h->h.handle(this.event));
	}
}
