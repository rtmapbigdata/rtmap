package cn.rtmap.flume.validation;

public class DummyValidator extends Validator {

	@Override
	public boolean validate(Object data) {
		return true;
	}
}
