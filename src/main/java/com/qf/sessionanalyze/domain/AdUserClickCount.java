package com.qf.sessionanalyze.domain;

import java.util.Objects;

/**
 * 用户广告点击量
 * @author Administrator
 *
 */
public class AdUserClickCount {

	private String date;
	private long userid;
	private long adid;
	private long clickCount;
	
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public long getUserid() {
		return userid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}
	public long getAdid() {
		return adid;
	}
	public void setAdid(long adid) {
		this.adid = adid;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AdUserClickCount that = (AdUserClickCount) o;
		return userid == that.userid &&
				adid == that.adid &&
				clickCount == that.clickCount &&
				Objects.equals(date, that.date);
	}

	@Override
	public int hashCode() {
		return Objects.hash(date, userid, adid, clickCount);
	}

	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
}
