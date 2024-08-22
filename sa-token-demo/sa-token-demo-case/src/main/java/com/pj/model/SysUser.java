package com.pj.model;

import java.io.Serializable;

/**
 * User 实体类 
 * 
 * @author click33
 * @since 2022-10-15
 */
public class SysUser implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2853125262828437774L;

	public SysUser() {
	}
	
	public SysUser(long id, String name, int age) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
	}
	

	/**
	 * 用户id
	 */
	private long id;
	
	/**
	 * 用户名称
	 */
	private String name;
	
	/**
	 * 用户年龄
	 */
	private int age;

	/**
	 * @return id
	 */
	public long getId() {
		return id;
	}

	/**
	 * @param id 要设置的 id
	 */
	public void setId(long id) {
		this.id = id;
	}

	/**
	 * @return name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name 要设置的 name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return age
	 */
	public int getAge() {
		return age;
	}

	/**
	 * @param age 要设置的 age
	 */
	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "SysUser [id=" + id + ", name=" + name + ", age=" + age + "]";
	}
	
}
